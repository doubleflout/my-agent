from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path

import uvicorn

from agent.config_models import Config
from bootstrap.tools import build_core_runtime
from core.net.http import SharedHttpResources
from webapp.agent_executor import AgentLoopExecutor
from webapp.app import create_web_app


async def run_web_chat_server(
    *,
    config: Config,
    workspace: Path,
    host: str = "0.0.0.0",
    port: int = 2240,
) -> None:
    http_resources = SharedHttpResources()
    core = build_core_runtime(config, workspace, http_resources)
    await core.start()
    app = create_web_app(
        workspace=workspace,
        agent_executor=AgentLoopExecutor(core.loop),
    )
    scheduler_task = asyncio.create_task(core.scheduler.run(), name="web_scheduler")
    server = uvicorn.Server(uvicorn.Config(app, host=host, port=port, log_level="info"))
    try:
        await server.serve()
    finally:
        core.scheduler.stop()
        scheduler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await scheduler_task
        await core.stop()
        await http_resources.aclose()
