from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from agent.tools.base import Tool
from webapp.store import WebStore


def _clean_skill_type(value: str) -> str:
    skill_type = str(value or "normal").strip()
    if skill_type not in {"normal", "drift", "tool"}:
        raise ValueError("skill_type must be one of: normal, drift, tool")
    return skill_type


def _default_relative_path(name: str, skill_type: str) -> str:
    return f"drift/skills/{name}" if skill_type == "drift" else f"skills/{name}"


def _is_safe_relative_path(path: str, skill_type: str) -> bool:
    clean = path.replace("\\", "/").strip().strip("/")
    if not clean or clean.startswith("../") or "/../" in clean:
        return False
    prefix = "drift/skills/" if skill_type == "drift" else "skills/"
    return clean.startswith(prefix)


class UpsertSkillRecordTool(Tool):
    name = "upsert_skill_record"
    description = (
        "在当前用户的 skills 业务表中新增或更新一条技能记录。"
        "创建或修改 SKILL.md 文件后调用，用于让前端和后台任务系统立即看到该技能。"
    )
    parameters = {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "技能名，通常等于技能目录名"},
            "skill_type": {
                "type": "string",
                "enum": ["normal", "drift", "tool"],
                "description": "normal=普通对话技能，drift=后台任务技能",
            },
            "relative_path": {
                "type": "string",
                "description": "技能目录相对当前用户 workspace 的路径，如 skills/foo 或 drift/skills/foo",
            },
            "title": {"type": "string", "description": "前端展示标题，可为空"},
            "description": {"type": "string", "description": "技能说明"},
            "enabled": {"type": "boolean", "description": "是否启用"},
            "metadata": {"type": "object", "description": "附加元数据，可为空"},
        },
        "required": ["name", "skill_type"],
    }

    def __init__(self, *, store: WebStore, workspace: Path, user_id: str) -> None:
        self._store = store
        self._workspace = workspace
        self._user_id = user_id

    async def execute(
        self,
        name: str,
        skill_type: str = "normal",
        relative_path: str = "",
        title: str | None = None,
        description: str = "",
        enabled: bool = True,
        metadata: dict[str, Any] | None = None,
        **_: Any,
    ) -> str:
        clean_name = str(name or "").strip()
        if not clean_name:
            return json.dumps({"ok": False, "error": "name is required"}, ensure_ascii=False)
        try:
            clean_type = _clean_skill_type(skill_type)
        except ValueError as exc:
            return json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False)
        rel = (relative_path or _default_relative_path(clean_name, clean_type)).replace("\\", "/")
        if not _is_safe_relative_path(rel, clean_type):
            return json.dumps(
                {"ok": False, "error": f"invalid relative_path for {clean_type}: {rel}"},
                ensure_ascii=False,
            )
        skill_file = self._workspace / rel / "SKILL.md"
        if not skill_file.exists():
            return json.dumps(
                {"ok": False, "error": f"SKILL.md not found: {rel}/SKILL.md"},
                ensure_ascii=False,
            )
        record = self._store.upsert_skill_record(
            user_id=self._user_id,
            name=clean_name,
            skill_type=clean_type,
            scope="user",
            title=title,
            description=description,
            source="filesystem",
            relative_path=rel,
            entry_file="SKILL.md",
            metadata=metadata or {},
            enabled=enabled,
        )
        return json.dumps(
            {
                "ok": True,
                "id": record.id,
                "name": record.name,
                "skill_type": record.skill_type,
                "relative_path": record.relative_path,
            },
            ensure_ascii=False,
        )


class DeleteSkillRecordTool(Tool):
    name = "delete_skill_record"
    description = "从当前用户的 skills 业务表中删除一条用户级技能记录，不删除文件。"
    parameters = {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "技能名"},
            "skill_type": {
                "type": "string",
                "enum": ["normal", "drift", "tool"],
                "description": "要删除的技能类型",
            },
        },
        "required": ["name", "skill_type"],
    }

    def __init__(self, *, store: WebStore, user_id: str) -> None:
        self._store = store
        self._user_id = user_id

    async def execute(self, name: str, skill_type: str = "normal", **_: Any) -> str:
        clean_name = str(name or "").strip()
        if not clean_name:
            return json.dumps({"ok": False, "error": "name is required"}, ensure_ascii=False)
        try:
            clean_type = _clean_skill_type(skill_type)
        except ValueError as exc:
            return json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False)
        deleted = self._store.delete_skill_record(
            user_id=self._user_id,
            name=clean_name,
            skill_type=clean_type,
            scope="user",
        )
        return json.dumps({"ok": deleted, "deleted": deleted}, ensure_ascii=False)
