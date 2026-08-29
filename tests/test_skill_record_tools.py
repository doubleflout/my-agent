from __future__ import annotations

import asyncio
import json
from pathlib import Path

from agent.tools.skill_records import DeleteSkillRecordTool, UpsertSkillRecordTool
from webapp.store import WebStore


def test_upsert_skill_record_tool_adds_and_updates_user_skill(tmp_path: Path):
    store = WebStore("sqlite:///" + (tmp_path / "web.db").as_posix())
    user = store.create_user(
        email="skill-tool@example.com",
        password_hash="hash",
        display_name="Skill Tool",
    )
    workspace = tmp_path / "users" / user.id
    skill_dir = workspace / "drift" / "skills" / "daily-review"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: daily-review\ndescription: 每日复盘\n---\n",
        encoding="utf-8",
    )
    tool = UpsertSkillRecordTool(
        store=store,
        workspace=workspace,
        user_id=user.id,
    )

    raw = asyncio.run(
        tool.execute(
            name="daily-review",
            skill_type="drift",
            relative_path="drift/skills/daily-review",
            title="每日复盘",
            description="根据聊天记录做复盘",
        )
    )

    result = json.loads(raw)
    assert result["ok"] is True
    records = store.list_skills(user_id=user.id)
    skill = next(item for item in records if item.name == "daily-review")
    assert skill.scope == "user"
    assert skill.skill_type == "drift"
    assert skill.title == "每日复盘"

    raw = asyncio.run(
        tool.execute(
            name="daily-review",
            skill_type="drift",
            relative_path="drift/skills/daily-review",
            title="每日复盘 v2",
            description="更新后的说明",
            enabled=False,
        )
    )

    result = json.loads(raw)
    assert result["ok"] is True
    updated = next(item for item in store.list_skills(user_id=user.id) if item.name == "daily-review")
    assert updated.title == "每日复盘 v2"
    assert updated.description == "更新后的说明"
    assert updated.enabled is False


def test_delete_skill_record_tool_removes_user_skill_record(tmp_path: Path):
    store = WebStore("sqlite:///" + (tmp_path / "web.db").as_posix())
    user = store.create_user(
        email="delete-skill@example.com",
        password_hash="hash",
        display_name="Delete Skill",
    )
    workspace = tmp_path / "users" / user.id
    skill_dir = workspace / "skills" / "temporary"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: temporary\ndescription: 临时技能\n---\n",
        encoding="utf-8",
    )
    upsert = UpsertSkillRecordTool(store=store, workspace=workspace, user_id=user.id)
    delete = DeleteSkillRecordTool(store=store, user_id=user.id)
    asyncio.run(
        upsert.execute(
            name="temporary",
            skill_type="normal",
            relative_path="skills/temporary",
            description="临时技能",
        )
    )

    raw = asyncio.run(delete.execute(name="temporary", skill_type="normal"))

    result = json.loads(raw)
    assert result["ok"] is True
    assert all(item.name != "temporary" for item in store.list_skills(user_id=user.id))
