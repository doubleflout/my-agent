from __future__ import annotations

import asyncio
import os
from pathlib import Path
from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from webapp.agent_executor import AgentExecutor, AgentLoopExecutor, web_session_key
from webapp.rate_limit import InMemoryRateLimiter, RateLimitExceeded, RateLimiter, RedisRateLimiter
from webapp.schemas import (
    ConversationResponse,
    CreateConversationRequest,
    CreateMessageRequest,
    CreateMessageResponse,
    LoginRequest,
    MessageResponse,
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
from webapp.store import DuplicateEmailError, UserRecord, WebStore, default_database_url


def _secret_from_env() -> str:
    return os.environ.get("AKASHIC_WEB_JWT_SECRET", "dev-secret-change-me")


def _database_url_from_env(workspace: Path) -> str:
    return os.environ.get("AKASHIC_WEB_DATABASE_URL") or default_database_url(workspace)


def create_web_app(
    *,
    workspace: Path,
    agent_executor: AgentExecutor | None = None,
    agent_loop=None,
    store: WebStore | None = None,
    jwt_secret: str | None = None,
) -> FastAPI:
    workspace.mkdir(parents=True, exist_ok=True)
    web_store = store or WebStore(_database_url_from_env(workspace))
    executor = agent_executor or AgentLoopExecutor(agent_loop)
    broker = TurnStreamBroker()
    limiter = _build_rate_limiter()
    secret = jwt_secret or _secret_from_env()

    app = FastAPI(title="Akashic Web Chat")
    app.state.web_store = web_store
    app.state.agent_executor = executor
    app.state.turn_stream_broker = broker
    app.state.rate_limiter = limiter

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
            init_user_workspace(workspace / "users" / user.id)
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
        return ConversationResponse(
            id=conv.id,
            title=conv.title,
            created_at=conv.created_at,
            updated_at=conv.updated_at,
        )

    @app.get("/api/conversations", response_model=list[ConversationResponse])
    async def list_conversations(
        user: UserRecord = Depends(get_current_user),
    ) -> list[ConversationResponse]:
        return [
            ConversationResponse(
                id=conv.id,
                title=conv.title,
                created_at=conv.created_at,
                updated_at=conv.updated_at,
            )
            for conv in web_store.list_conversations(user_id=user.id)
        ]

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
        content: str,
    ) -> None:
        try:
            web_store.update_turn(turn_id=turn_id, status="running")
            response = await executor.run(
                content=content,
                user_id=user_id,
                conversation_id=conversation_id,
            )
            web_store.add_message(
                conversation_id=conversation_id,
                user_id=user_id,
                role="assistant",
                content=response,
                metadata={"turn_id": turn_id},
            )
            web_store.update_turn(turn_id=turn_id, status="completed")
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
        if web_store.get_conversation(user_id=user.id, conversation_id=conversation_id) is None:
            raise HTTPException(status_code=404, detail="conversation not found")
        acquired = False
        try:
            await limiter.check_minute(user.id)
            await limiter.acquire_turn(user.id)
            acquired = True
        except RateLimitExceeded as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        try:
            message = web_store.add_message(
                conversation_id=conversation_id,
                user_id=user.id,
                role="user",
                content=payload.content,
                metadata={},
            )
            turn = web_store.create_turn(user_id=user.id, conversation_id=conversation_id)
            asyncio.create_task(
                run_turn(
                    turn_id=turn.id,
                    user_id=user.id,
                    conversation_id=conversation_id,
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
                id=message.id,
                conversation_id=message.conversation_id,
                role=message.role,
                content=message.content,
                metadata=message.metadata,
                created_at=message.created_at,
            ),
            turn_id=turn.id,
            session_key=web_session_key(user.id, conversation_id),
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
