from __future__ import annotations

from pathlib import Path

from agent.tools.registry import ToolRegistry
from agent.tools.skill_records import DeleteSkillRecordTool, UpsertSkillRecordTool
from bootstrap.toolsets.protocol import (
    ToolsetDeps,
    ToolsetProvider,
    build_registration_result,
)
from webapp.store import WebStore, database_url_from_config


def _workspace_user_id(workspace: Path) -> str | None:
    if workspace.parent.name != "users":
        return None
    user_id = workspace.name.strip()
    return user_id or None


class SkillRecordsToolsetProvider(ToolsetProvider):
    def register(self, registry: ToolRegistry, deps: ToolsetDeps):
        before = set(registry._tools.keys())
        user_id = _workspace_user_id(deps.workspace)
        if not user_id:
            return build_registration_result(
                registry=registry,
                source_name="skill_records",
                before=before,
            )
        store = WebStore(database_url_from_config(deps.config, deps.workspace))
        registry.register(
            UpsertSkillRecordTool(
                store=store,
                workspace=deps.workspace,
                user_id=user_id,
            ),
            risk="write",
            search_hint="创建技能 更新技能 同步技能表 skill 业务表 drift 后台任务",
        )
        registry.register(
            DeleteSkillRecordTool(store=store, user_id=user_id),
            risk="write",
            search_hint="删除技能 移除技能 skill 业务表",
        )
        return build_registration_result(
            registry=registry,
            source_name="skill_records",
            before=before,
        )
