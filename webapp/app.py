from __future__ import annotations

import asyncio
import inspect
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from agent.config_models import Config
from webapp.agent_executor import AgentExecutor, AgentLoopExecutor
from webapp.proactive_scheduler import WebProactiveRunner, WebProactiveScheduler
from webapp.rate_limit import InMemoryRateLimiter, RateLimitExceeded, RateLimiter, RedisRateLimiter
from webapp.schemas import (
    ConversationResponse,
    CreateConversationRequest,
    CreateMessageRequest,
    CreateMessageResponse,
    LoginRequest,
    MessageResponse,
    MessageSourceResponse,
    RegisterRequest,
    TokenResponse,
    UserResponse,
)
from bootstrap.init_workspace import init_user_workspace
from webapp.security import (
    TokenError,
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)
from webapp.sse import TurnStreamBroker
from webapp.runtime_manager import UserWorkspaceResolver
from webapp.store import DuplicateEmailError, UserRecord, WebStore, database_url_from_config


def _executor_accepts_stream_events(executor: AgentExecutor) -> bool:
    try:
        signature = inspect.signature(executor.run)
    except (TypeError, ValueError):
        return True
    return (
        "on_stream_event" in signature.parameters
        or any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values())
    )


def _secret_from_env() -> str:
    return os.environ.get("AKASHIC_WEB_JWT_SECRET", "dev-secret-change-me")


def create_web_app(
    *,
    workspace: Path,
    agent_executor: AgentExecutor | None = None,
    proactive_runner: WebProactiveRunner | None = None,
    agent_loop=None,
    store: WebStore | None = None,
    jwt_secret: str | None = None,
) -> FastAPI:
    workspace.mkdir(parents=True, exist_ok=True)
    workspace_resolver = UserWorkspaceResolver(workspace)
    try:
        app_config = Config.load("config.toml")
    except Exception:
        app_config = None
    web_store = store or WebStore(database_url_from_config(app_config, workspace))
    executor = agent_executor or AgentLoopExecutor(agent_loop)
    broker = TurnStreamBroker()
    limiter = _build_rate_limiter()
    secret = jwt_secret or _secret_from_env()

    app = FastAPI(title="Akashic Web Chat")
    app.state.web_store = web_store
    app.state.agent_executor = executor
    app.state.turn_stream_broker = broker
    app.state.rate_limiter = limiter
    app.state.web_proactive_scheduler = None
    app.state.web_proactive_task = None

    if app_config is not None and proactive_runner is not None and app_config.proactive.enabled:
        proactive_scheduler = WebProactiveScheduler(
            store=web_store,
            config=app_config,
            runner=proactive_runner,
        )
        app.state.web_proactive_scheduler = proactive_scheduler

        @app.on_event("startup")
        async def start_web_proactive_scheduler() -> None:
            app.state.web_proactive_task = asyncio.create_task(
                proactive_scheduler.run(),
                name="web_proactive_scheduler",
            )

        @app.on_event("shutdown")
        async def stop_web_proactive_scheduler() -> None:
            proactive_scheduler.stop()
            task = app.state.web_proactive_task
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    origins = [
        origin.strip()
        for origin in os.environ.get("AKASHIC_WEB_CORS_ORIGINS", "*").split(",")
        if origin.strip()
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def user_to_response(user: UserRecord) -> UserResponse:
        return UserResponse(
            id=user.id,
            email=user.email,
            display_name=user.display_name,
            created_at=user.created_at,
        )

    def conversation_to_response(conv) -> ConversationResponse:
        return ConversationResponse(
            id=conv.id,
            title=conv.title,
            session_key=conv.session_key,
            created_at=conv.created_at,
            updated_at=conv.updated_at,
        )

    def load_message_sources(user_id: str) -> list[MessageSourceResponse]:
        path = workspace_resolver.for_user(user_id) / "proactive_sources.json"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return []
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"failed to read proactive sources: {exc}") from exc
        raw_sources = data.get("sources", []) if isinstance(data, dict) else data
        if not isinstance(raw_sources, list):
            return []
        sources: list[MessageSourceResponse] = []
        for index, raw in enumerate(raw_sources):
            if not isinstance(raw, dict):
                continue
            source_id = str(raw.get("id") or "").strip()
            if not source_id:
                source_id = f"source-{index + 1}"
            sources.append(
                MessageSourceResponse(
                    id=source_id,
                    name=str(raw.get("name") or source_id),
                    type=str(raw.get("type") or "content"),
                    enabled=bool(raw.get("enabled", True)),
                    server=str(raw.get("server") or "") or None,
                    get_tool=str(raw.get("get_tool") or "") or None,
                    ack_tool=str(raw.get("ack_tool") or "") or None,
                    description=str(raw.get("description") or "") or None,
                )
            )
        return sources

    def get_current_user(authorization: str | None = Header(default=None)) -> UserRecord:
        if not authorization or not authorization.lower().startswith("bearer "):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing token")
        token = authorization.split(" ", 1)[1].strip()
        try:
            claims = decode_access_token(token, secret=secret)
        except TokenError as exc:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
        user = web_store.get_user(claims.sub)
        if user is None or user.disabled:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid user")
        return user

    @app.post("/api/auth/register", response_model=TokenResponse)
    async def register(payload: RegisterRequest) -> TokenResponse:
        if len(payload.password.strip()) < 8:
            raise HTTPException(status_code=400, detail="password too short")
        try:
            user = web_store.create_user(
                email=str(payload.email),
                password_hash=hash_password(payload.password),
                display_name=payload.display_name,
            )
            init_user_workspace(
                workspace_resolver.for_user(user.id),
                config=app_config,
            )
            web_store.ensure_default_proactive_session(user_id=user.id)
        except DuplicateEmailError as exc:
            raise HTTPException(status_code=409, detail="email already registered") from exc
        return TokenResponse(access_token=create_access_token(user_id=user.id, secret=secret))

    @app.post("/api/auth/login", response_model=TokenResponse)
    async def login(payload: LoginRequest) -> TokenResponse:
        user = web_store.get_user_by_email(str(payload.email))
        if user is None or user.disabled or not verify_password(payload.password, user.password_hash):
            raise HTTPException(status_code=401, detail="invalid credentials")
        return TokenResponse(access_token=create_access_token(user_id=user.id, secret=secret))

    @app.get("/api/auth/me", response_model=UserResponse)
    async def me(user: UserRecord = Depends(get_current_user)) -> UserResponse:
        return user_to_response(user)

    @app.post("/api/conversations", response_model=ConversationResponse)
    async def create_conversation(
        payload: CreateConversationRequest,
        user: UserRecord = Depends(get_current_user),
    ) -> ConversationResponse:
        conv = web_store.create_conversation(user_id=user.id, title=payload.title)
        return conversation_to_response(conv)

    @app.get("/api/conversations", response_model=list[ConversationResponse])
    async def list_conversations(
        user: UserRecord = Depends(get_current_user),
    ) -> list[ConversationResponse]:
        return [conversation_to_response(conv) for conv in web_store.list_conversations(user_id=user.id)]

    @app.get("/api/proactive/conversation", response_model=ConversationResponse)
    async def get_proactive_conversation(
        user: UserRecord = Depends(get_current_user),
    ) -> ConversationResponse:
        conv = web_store.get_default_proactive_conversation(user_id=user.id)
        if conv is None:
            conv = web_store.ensure_default_proactive_session(user_id=user.id)
        return conversation_to_response(conv)

    @app.get("/api/proactive/sources", response_model=list[MessageSourceResponse])
    async def list_proactive_sources(
        user: UserRecord = Depends(get_current_user),
    ) -> list[MessageSourceResponse]:
        return load_message_sources(user.id)

    @app.get(
        "/api/conversations/{conversation_id}/messages",
        response_model=list[MessageResponse],
    )
    async def list_messages(
        conversation_id: str,
        user: UserRecord = Depends(get_current_user),
    ) -> list[MessageResponse]:
        if web_store.get_conversation(user_id=user.id, conversation_id=conversation_id) is None:
            raise HTTPException(status_code=404, detail="conversation not found")
        return [
            MessageResponse(
                id=msg.id,
                conversation_id=msg.conversation_id,
                role=msg.role,
                content=msg.content,
                metadata=msg.metadata,
                created_at=msg.created_at,
            )
            for msg in web_store.list_messages(user_id=user.id, conversation_id=conversation_id)
        ]

    async def run_turn(
        *,
        turn_id: str,
        user_id: str,
        conversation_id: str,
        session_key: str,
        content: str,
    ) -> None:
        streamed_content = False

        async def publish_stream(delta: dict[str, str]) -> None:
            nonlocal streamed_content
            content_delta = str(delta.get("content_delta") or "")
            thinking_delta = str(delta.get("thinking_delta") or "")
            if thinking_delta:
                await broker.publish(
                    turn_id,
                    {"event": "thinking_delta", "data": {"text": thinking_delta}},
                )
            if content_delta:
                streamed_content = True
                await broker.publish(
                    turn_id,
                    {"event": "content_delta", "data": {"text": content_delta}},
                )

        try:
            web_store.update_turn(turn_id=turn_id, status="running")
            run_kwargs: dict[str, Any] = {
                "content": content,
                "user_id": user_id,
                "conversation_id": conversation_id,
                "session_key": session_key,
            }
            if _executor_accepts_stream_events(executor):
                run_kwargs["on_stream_event"] = publish_stream
            response = await executor.run(**run_kwargs)
            web_store.update_turn(turn_id=turn_id, status="completed")
            if response and not streamed_content:
                await broker.publish(turn_id, {"event": "content_delta", "data": {"text": response}})
            await broker.publish(turn_id, {"event": "done", "data": {"turn_id": turn_id}})
        except Exception as exc:
            web_store.update_turn(turn_id=turn_id, status="failed", error=str(exc))
            await broker.publish(turn_id, {"event": "error", "data": {"message": str(exc)}})
        finally:
            await limiter.release_turn(user_id)

    @app.post(
        "/api/conversations/{conversation_id}/messages",
        response_model=CreateMessageResponse,
    )
    async def create_message(
        conversation_id: str,
        payload: CreateMessageRequest,
        user: UserRecord = Depends(get_current_user),
    ) -> CreateMessageResponse:
        conv = web_store.get_conversation(user_id=user.id, conversation_id=conversation_id)
        if conv is None:
            raise HTTPException(status_code=404, detail="conversation not found")
        acquired = False
        try:
            await limiter.check_minute(user.id)
            await limiter.acquire_turn(user.id)
            acquired = True
        except RateLimitExceeded as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        try:
            turn = web_store.create_turn(user_id=user.id, conversation_id=conversation_id)
            asyncio.create_task(
                run_turn(
                    turn_id=turn.id,
                    user_id=user.id,
                    conversation_id=conversation_id,
                    session_key=conv.session_key,
                    content=payload.content,
                ),
                name=f"web_turn:{turn.id}",
            )
        except Exception:
            if acquired:
                await limiter.release_turn(user.id)
            raise
        return CreateMessageResponse(
            message=MessageResponse(
                id=f"pending:{uuid.uuid4()}",
                conversation_id=conversation_id,
                role="user",
                content=payload.content,
                metadata={"pending": True},
                created_at=datetime.now(timezone.utc),
            ),
            turn_id=turn.id,
            session_key=conv.session_key,
        )

    @app.get("/api/turns/{turn_id}/stream")
    async def stream_turn(
        turn_id: str,
        user: UserRecord = Depends(get_current_user),
    ) -> StreamingResponse:
        turn = web_store.get_turn(user_id=user.id, turn_id=turn_id)
        if turn is None:
            raise HTTPException(status_code=404, detail="turn not found")
        return StreamingResponse(
            broker.stream(turn_id),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    static_dir = Path(__file__).resolve().parent.parent / "static" / "web"
    if static_dir.exists():
        @app.get("/")
        async def web_index() -> FileResponse:
            return FileResponse(static_dir / "index.html")

        app.mount("/", StaticFiles(directory=static_dir), name="web-static")

    return app


def _build_rate_limiter() -> RateLimiter:
    max_per_minute = int(os.environ.get("AKASHIC_WEB_RATE_PER_MINUTE", "20"))
    max_concurrent = int(os.environ.get("AKASHIC_WEB_MAX_CONCURRENT_TURNS", "2"))
    redis_url = os.environ.get("AKASHIC_WEB_REDIS_URL", "").strip()
    if redis_url:
        return RedisRateLimiter(
            redis_url,
            max_per_minute=max_per_minute,
            max_concurrent=max_concurrent,
        )
    return InMemoryRateLimiter(max_per_minute=max_per_minute, max_concurrent=max_concurrent)
