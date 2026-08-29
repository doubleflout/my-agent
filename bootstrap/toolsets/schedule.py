from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from agent.scheduler import LatencyTracker, SchedulerService
from agent.scheduler_postgres_store import PostgresScheduleStore
from agent.tools.message_push import MessagePushTool
from agent.tools.registry import ToolRegistry
from agent.tools.schedule import CancelScheduleTool, ListSchedulesTool, ScheduleTool
from bootstrap.toolsets.protocol import (
    ToolsetDeps,
    ToolsetProvider,
    build_registration_result,
)


def _workspace_user_id(workspace: Path) -> str | None:
    if workspace.parent.name != "users":
        return None
    user_id = workspace.name.strip()
    return user_id or None


class SchedulerToolsetProvider(ToolsetProvider):
    def register(self, registry: ToolRegistry, deps: ToolsetDeps):
        before = set(registry._tools.keys())
        scheduler = deps.scheduler
        if scheduler is None:
            raise RuntimeError("SchedulerToolsetProvider requires scheduler")
        registry.register(
            ScheduleTool(scheduler),
            risk="write",
            search_hint="cron timer 延时执行",
        )
        registry.register(
            ListSchedulesTool(scheduler),
            risk="read-only",
            search_hint="提醒列表 已有计划",
        )
        registry.register(
            CancelScheduleTool(scheduler),
            risk="write",
            search_hint="删除提醒 取消任务",
        )
        return build_registration_result(
            registry=registry,
            source_name="schedule",
            before=before,
            extras={"scheduler": scheduler},
        )


def build_scheduler(
    workspace: Path,
    push_tool: MessagePushTool,
    *,
    config: Any | None = None,
    agent_loop_provider: Callable[[], Any] | None = None,
) -> SchedulerService:
    schedule_store = None
    storage = getattr(config, "storage", None)
    if str(getattr(storage, "backend", "")).lower() == "postgres":
        from webapp.store import database_url_from_config

        schedule_store = PostgresScheduleStore(
            database_url_from_config(config, workspace),
            user_id=_workspace_user_id(workspace),
        )
    return SchedulerService(
        store_path=workspace / "schedules.json",
        push_tool=push_tool,
        agent_loop=None,
        agent_loop_provider=agent_loop_provider,
        tracker=LatencyTracker(),
        schedule_store=schedule_store,
    )


def register_scheduler_tools(
    tools: ToolRegistry,
    scheduler: SchedulerService,
) -> None:
    SchedulerToolsetProvider().register(
        tools,
        ToolsetDeps(
            config=None,
            workspace=Path("."),
            scheduler=scheduler,
        ),
    )
