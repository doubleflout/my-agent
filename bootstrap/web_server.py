from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path

import uvicorn

from agent.config_models import Config
from core.net.http import SharedHttpResources
from webapp.app import create_web_app
from webapp.runtime_manager import UserRuntimeAgentExecutor, UserRuntimeManager


async def run_web_chat_server(
    *,
    config: Config,
    workspace: Path,
    host: str = "0.0.0.0",
    port: int = 2240,
) -> None:
    http_resources = SharedHttpResources()
    runtime_manager = UserRuntimeManager(
        config=config,
        base_workspace=workspace,
        http_resources=http_resources,
    )
    app = create_web_app(
        workspace=workspace,
        agent_executor=UserRuntimeAgentExecutor(runtime_manager),
    )
    server = uvicorn.Server(uvicorn.Config(app, host=host, port=port, log_level="info"))
    try:
        await server.serve()
    finally:
        with contextlib.suppress(asyncio.CancelledError):
            await runtime_manager.aclose()
        await http_resources.aclose()
