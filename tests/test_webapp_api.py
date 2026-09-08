from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
from sqlalchemy import text

from webapp.agent_executor import web_session_key
from webapp.app import create_web_app
from webapp.store import WebStore


def test_skill_detail_reads_user_workspace(tmp_path):
    from webapp.runtime_manager import UserWorkspaceResolver

    async def scenario():
        app = make_app(tmp_path)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            token = await register(client, "skill-owner@example.com")
            other = await register(client, "skill-other@example.com")
            headers = {"Authorization": f"Bearer {token}"}
            user = (await client.get("/api/auth/me", headers=headers)).json()
            root = UserWorkspaceResolver(tmp_path).for_user(user["id"])
            path = root / "skills" / "private-detail" / "SKILL.md"
            path.parent.mkdir(parents=True, exist_ok=True)
            content = "---\nname: private-detail\ndescription: Private skill\n---\nPrivate instructions"
            path.write_text(content, encoding="utf-8")
            records = (await client.get("/api/skills", headers=headers)).json()
            skill = next(item for item in records if item["name"] == "private-detail")
            url = f"/api/skills/{skill['id']}"
            assert (await client.get(url, headers=headers)).json()["content"] == content
            assert (await client.get(url)).status_code == 401
            assert (await client.get(url, headers={"Authorization": f"Bearer {other}"})).status_code == 404
    asyncio.run(scenario())


def test_memory_api_isolation(tmp_path):
    from memory2.store import MemoryStore2
    from webapp.runtime_manager import UserWorkspaceResolver

    async def scenario():
        app = make_app(tmp_path)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/api/memory/items")).status_code == 401
            first = await register(client, "memory-a@example.com")
            second = await register(client, "memory-b@example.com")
            headers = {"Authorization": f"Bearer {first}"}
            other = {"Authorization": f"Bearer {second}"}
            user = (await client.get("/api/auth/me", headers=headers)).json()
            root = UserWorkspaceResolver(tmp_path).for_user(user["id"]) / "memory"
            root.mkdir(parents=True, exist_ok=True)
            (root / "MEMORY.md").write_text("Private profile", encoding="utf-8")
            memory = MemoryStore2(root / "memory2.db")
            item_id = memory.upsert_item("profile", "Private fact", None).split(":", 1)[1]
            memory.close()
            assert (await client.get("/api/memory/profile", headers=headers)).json()["content"] == "Private profile"
            result = (await client.get("/api/memory/items?q=Private&memory_type=profile", headers=headers)).json()
            assert result["total"] == 1
            assert (await client.get(f"/api/memory/items/{item_id}", headers=headers)).status_code == 200
            assert (await client.get(f"/api/memory/items/{item_id}", headers=other)).status_code == 404
            assert (await client.get("/api/memory/items", headers=other)).json()["total"] == 0
            assert (await client.get("/api/memory/items?page=0", headers=headers)).status_code == 422
    asyncio.run(scenario())


class FakeExecutor:
    def __init__(self, *, fail: bool = False, delay: float = 0.0) -> None:
        self.fail = fail
        self.delay = delay
        self.calls: list[dict[str, str]] = []

    async def run(
        self,
        *,
        content: str,
        user_id: str,
        conversation_id: str,
        session_key: str | None = None,
        turn_id: str | None = None,
    ) -> str:
        self.calls.append(
            {
                "content": content,
                "user_id": user_id,
                "conversation_id": conversation_id,
                "session_key": session_key or "",
                "turn_id": turn_id or "",
            }
        )
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail:
            raise RuntimeError("agent failed")
        return f"echo: {content}"


class StreamingFakeExecutor(FakeExecutor):
    async def run(
        self,
        *,
        content: str,
        user_id: str,
        conversation_id: str,
        session_key: str | None = None,
        turn_id: str | None = None,
        on_stream_event=None,
    ) -> str:
        self.calls.append(
            {
                "content": content,
                "user_id": user_id,
                "conversation_id": conversation_id,
                "session_key": session_key or "",
                "turn_id": turn_id or "",
            }
        )
        if on_stream_event is not None:
            await on_stream_event({"thinking_delta": "先想一下"})
            await on_stream_event({"content_delta": "你好"})
            await on_stream_event({"content_delta": "呀"})
        return "你好呀"


def make_app(tmp_path: Path, executor: FakeExecutor | None = None):
    store = WebStore("sqlite:///" + (tmp_path / "web.db").as_posix())
    return create_web_app(
        workspace=tmp_path,
        store=store,
        agent_executor=executor or FakeExecutor(),
        jwt_secret="test-secret",
    )


async def register(client: httpx.AsyncClient, email: str) -> str:
    res = await client.post(
        "/api/auth/register",
        json={"email": email, "password": "password123", "display_name": "Tester"},
    )
    assert res.status_code == 200, res.text
    return res.json()["access_token"]


async def create_conversation(client: httpx.AsyncClient, token: str) -> str:
    res = await client.post(
        "/api/conversations",
        headers={"Authorization": f"Bearer {token}"},
        json={"title": "Daily"},
    )
    assert res.status_code == 200, res.text
    return res.json()["id"]


async def wait_for_executor_call(executor: FakeExecutor, count: int = 1) -> None:
    for _ in range(50):
        if len(executor.calls) >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"expected {count} executor calls, got {len(executor.calls)}")


async def test_register_login_and_me(tmp_path):
    app = make_app(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        token = await register(client, "User@Example.com")
        me = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert me.status_code == 200
        assert me.json()["email"] == "user@example.com"

        duplicate = await client.post(
            "/api/auth/register",
            json={"email": "user@example.com", "password": "password123"},
        )
        assert duplicate.status_code == 409

        bad_login = await client.post(
            "/api/auth/login",
            json={"email": "user@example.com", "password": "wrong"},
        )
        assert bad_login.status_code == 401

        login = await client.post(
            "/api/auth/login",
            json={"email": "user@example.com", "password": "password123"},
        )
        assert login.status_code == 200


async def test_proactive_conversation_endpoint_returns_default_session(tmp_path):
    app = make_app(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        token = await register(client, "proactive@example.com")

        res = await client.get(
            "/api/proactive/conversation",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert res.status_code == 200, res.text
        body = res.json()
        assert body["title"] == "主动推送"
        assert body["session_key"].startswith("web:proactive:")


async def test_proactive_sources_are_loaded_from_user_workspace(tmp_path):
    app = make_app(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        token = await register(client, "sources@example.com")
        me = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
        user_id = me.json()["id"]
        source_path = tmp_path / "users" / user_id / "proactive_sources.json"
        source_path.write_text(
            json.dumps(
                {
                    "sources": [
                        {
                            "id": "bilibili-fitness-hot",
                            "name": "B站健身热点",
                            "type": "content",
                            "enabled": True,
                            "server": "bilibili-fitness",
                            "get_tool": "get_fitness_proactive_events",
                            "ack_tool": "acknowledge_events",
                            "description": "B站运动分区热门榜健身内容",
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        res = await client.get(
            "/api/proactive/sources",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert res.status_code == 200, res.text
    assert res.json() == [
        {
            "id": "bilibili-fitness-hot",
            "name": "B站健身热点",
            "type": "content",
            "enabled": True,
            "server": "bilibili-fitness",
            "get_tool": "get_fitness_proactive_events",
            "ack_tool": "acknowledge_events",
            "description": "B站运动分区热门榜健身内容",
        }
    ]


async def test_schedules_are_loaded_from_user_workspace(tmp_path):
    app = make_app(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        token = await register(client, "schedules@example.com")
        me = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
        user_id = me.json()["id"]
        schedule_path = tmp_path / "users" / user_id / "schedules.json"
        schedule_path.write_text(
            json.dumps(
                [
                    {
                        "id": "00000000-0000-0000-0000-000000000001",
                        "name": "每日复盘",
                        "trigger": "every",
                        "tier": "soft",
                        "fire_at": "2026-08-29T09:00:00+08:00",
                        "timezone": "Asia/Shanghai",
                        "channel": "web",
                        "chat_id": "conversation-1",
                        "session_key": f"web:{user_id}:conversation-1",
                        "prompt": "根据昨天聊天做复盘",
                        "run_count": 2,
                        "enabled": True,
                    }
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        res = await client.get(
            "/api/schedules",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert res.status_code == 200, res.text
    assert res.json() == [
        {
            "id": "00000000-0000-0000-0000-000000000001",
            "name": "每日复盘",
            "trigger": "every",
            "tier": "soft",
            "enabled": True,
            "fire_at": "2026-08-29T09:00:00+08:00",
            "timezone": "Asia/Shanghai",
            "channel": "web",
            "chat_id": "conversation-1",
            "session_key": f"web:{user_id}:conversation-1",
            "run_count": 2,
            "action_preview": "根据昨天聊天做复盘",
        }
    ]


async def test_skills_endpoint_syncs_global_and_user_workspace_skills(tmp_path):
    app = make_app(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        token = await register(client, "skills@example.com")
        me = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
        user_id = me.json()["id"]
        user_workspace = tmp_path / "users" / user_id
        normal_skill = user_workspace / "skills" / "user-normal"
        normal_skill.mkdir(parents=True)
        (normal_skill / "SKILL.md").write_text(
            "---\nname: user-normal\ndescription: 用户普通技能\n---\n\nbody",
            encoding="utf-8",
        )
        drift_skill = user_workspace / "drift" / "skills" / "user-drift"
        drift_skill.mkdir(parents=True)
        (drift_skill / "SKILL.md").write_text(
            "---\nname: user-drift\ndescription: 用户后台任务技能\n---\n\nbody",
            encoding="utf-8",
        )

        res = await client.get(
            "/api/skills",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert res.status_code == 200, res.text
    rows = res.json()
    by_name = {row["name"]: row for row in rows}
    assert by_name["user-normal"]["scope"] == "user"
    assert by_name["user-normal"]["skill_type"] == "normal"
    assert by_name["user-normal"]["relative_path"] == "skills/user-normal"
    assert by_name["user-drift"]["scope"] == "user"
    assert by_name["user-drift"]["skill_type"] == "drift"
    assert by_name["user-drift"]["relative_path"] == "drift/skills/user-drift"
    assert by_name["weather"]["scope"] == "global"


async def test_user_skill_enabled_can_be_toggled_but_global_skill_cannot(tmp_path):
    app = make_app(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        token = await register(client, "toggle-skill@example.com")
        me = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
        user_id = me.json()["id"]
        user_workspace = tmp_path / "users" / user_id
        skill_dir = user_workspace / "skills" / "user-toggle"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text(
            "---\nname: user-toggle\ndescription: 用户可启停技能\n---\n",
            encoding="utf-8",
        )

        listed = await client.get("/api/skills", headers={"Authorization": f"Bearer {token}"})
        assert listed.status_code == 200, listed.text
        by_name = {row["name"]: row for row in listed.json()}

        disabled = await client.patch(
            f"/api/skills/{by_name['user-toggle']['id']}",
            headers={"Authorization": f"Bearer {token}"},
            json={"enabled": False},
        )
        blocked = await client.patch(
            f"/api/skills/{by_name['weather']['id']}",
            headers={"Authorization": f"Bearer {token}"},
            json={"enabled": False},
        )

    assert disabled.status_code == 200, disabled.text
    assert disabled.json()["enabled"] is False
    assert disabled.json()["scope"] == "user"
    assert blocked.status_code == 403


async def test_background_tasks_endpoint_groups_drift_ticks_by_skill(tmp_path):
    app = make_app(tmp_path)
    store: WebStore = app.state.web_store
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        token = await register(client, "background-tasks@example.com")
        me = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
        user_id = me.json()["id"]
        drift_skill = tmp_path / "users" / user_id / "drift" / "skills" / "daily-review"
        drift_skill.mkdir(parents=True)
        (drift_skill / "SKILL.md").write_text(
            "---\nname: daily-review\ndescription: 每日复盘后台任务\n---\n",
            encoding="utf-8",
        )
        with store.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE tick_log (
                        tick_id TEXT PRIMARY KEY,
                        session_key TEXT NOT NULL,
                        user_id TEXT,
                        started_at TEXT NOT NULL,
                        finished_at TEXT,
                        gate_exit TEXT,
                        terminal_action TEXT,
                        skip_reason TEXT,
                        steps_taken INTEGER,
                        drift_entered BOOLEAN,
                        final_message TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE tick_step_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        tick_id TEXT NOT NULL,
                        step_index INTEGER NOT NULL,
                        phase TEXT NOT NULL,
                        tool_name TEXT NOT NULL,
                        tool_call_id TEXT NOT NULL,
                        tool_args_json TEXT NOT NULL,
                        tool_result_text TEXT NOT NULL,
                        terminal_action_after TEXT,
                        skip_reason_after TEXT,
                        interesting_ids_after TEXT NOT NULL DEFAULT '[]',
                        discarded_ids_after TEXT NOT NULL DEFAULT '[]',
                        cited_ids_after TEXT NOT NULL DEFAULT '[]',
                        final_message_after TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO tick_log(
                        tick_id, session_key, user_id, started_at, finished_at,
                        terminal_action, skip_reason, steps_taken, drift_entered, final_message
                    )
                    VALUES(
                        'tick-1', :session_key, :user_id, '2026-08-31T08:00:00+00:00',
                        '2026-08-31T08:01:00+00:00', 'reply', '', 4, 1, '后台任务已推进'
                    )
                    """
                ),
                {"session_key": f"web:proactive:{user_id}:conv-1", "user_id": user_id},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO tick_log(
                        tick_id, session_key, user_id, started_at, finished_at,
                        terminal_action, skip_reason, steps_taken, drift_entered, final_message
                    )
                    VALUES(
                        'tick-2', :session_key, :user_id, '2026-08-31T09:00:00+00:00',
                        '2026-08-31T09:01:00+00:00', 'skip', 'no_content', 2, 1, '后台任务最新推进'
                    )
                    """
                ),
                {"session_key": f"web:proactive:{user_id}:conv-1", "user_id": user_id},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO tick_step_log(
                        tick_id, step_index, phase, tool_name, tool_call_id, tool_args_json, tool_result_text
                    )
                    VALUES
                        ('tick-1', 1, 'drift', 'finish_drift', 'call-1', :args, '{}'),
                        ('tick-2', 1, 'drift', 'finish_drift', 'call-2', :args, '{}')
                    """
                ),
                {"args": json.dumps({"skill_used": "daily-review"}, ensure_ascii=False)},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO tick_log(
                        tick_id, session_key, user_id, started_at, drift_entered, final_message
                    )
                    VALUES('tick-other', 'web:proactive:other:conv-2', 'other', '2026-08-31T08:02:00+00:00', 1, '别人的任务')
                    """
                )
            )

        res = await client.get(
            "/api/background-tasks",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert res.status_code == 200, res.text
    rows = res.json()
    assert len(rows) == 1
    assert rows[0]["id"] not in {"tick-1", "tick-2"}
    assert rows[0]["name"] == "daily-review"
    assert rows[0]["description"] == "每日复盘后台任务"
    assert rows[0]["enabled"] is True
    assert rows[0]["session_key"] == f"web:proactive:{user_id}:conv-1"
    assert rows[0]["status"] == "skip"
    assert rows[0]["summary"] == "后台任务最新推进"
    assert rows[0]["started_at"] == "2026-08-31T09:00:00Z"
    assert rows[0]["finished_at"] == "2026-08-31T09:01:00Z"
    assert rows[0]["steps_taken"] == 2


async def test_background_tasks_endpoint_ignores_unmatched_tick_ids(tmp_path):
    app = make_app(tmp_path)
    store: WebStore = app.state.web_store
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        token = await register(client, "background-no-finish@example.com")
        me = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
        user_id = me.json()["id"]
        drift_skill = tmp_path / "users" / user_id / "drift" / "skills" / "daily-review"
        drift_skill.mkdir(parents=True)
        (drift_skill / "SKILL.md").write_text(
            "---\nname: daily-review\ndescription: 每日复盘后台任务\n---\n",
            encoding="utf-8",
        )
        with store.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE tick_log (
                        tick_id TEXT PRIMARY KEY,
                        session_key TEXT NOT NULL,
                        user_id TEXT,
                        started_at TEXT NOT NULL,
                        finished_at TEXT,
                        terminal_action TEXT,
                        skip_reason TEXT,
                        steps_taken INTEGER,
                        drift_entered BOOLEAN,
                        final_message TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO tick_log(
                        tick_id, session_key, user_id, started_at, finished_at,
                        terminal_action, skip_reason, steps_taken, drift_entered, final_message
                    )
                    VALUES
                        ('tick-a', :session_key, :user_id, '2026-08-31T08:00:00+00:00', NULL, 'skip', 'no_content', 1, 1, ''),
                        ('tick-b', :session_key, :user_id, '2026-08-31T09:00:00+00:00', NULL, 'skip', 'no_content', 1, 1, '')
                    """
                ),
                {"session_key": f"web:proactive:{user_id}:conv-1", "user_id": user_id},
            )

        res = await client.get(
            "/api/background-tasks",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert res.status_code == 200, res.text
    rows = res.json()
    assert len(rows) == 1
    assert rows[0]["name"] == "daily-review"
    assert rows[0]["id"] not in {"tick-a", "tick-b"}
    assert rows[0]["status"] == "idle"


async def test_schedule_enabled_can_be_toggled_in_user_workspace(tmp_path):
    app = make_app(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        token = await register(client, "toggle-schedule@example.com")
        me = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
        user_id = me.json()["id"]
        schedule_path = tmp_path / "users" / user_id / "schedules.json"
        schedule_path.write_text(
            json.dumps(
                [
                    {
                        "id": "00000000-0000-0000-0000-000000000002",
                        "name": "每日复盘",
                        "trigger": "every",
                        "tier": "soft",
                        "fire_at": "2026-08-29T09:00:00+08:00",
                        "timezone": "Asia/Shanghai",
                        "session_key": f"web:{user_id}:conversation-1",
                        "prompt": "根据昨天聊天做复盘",
                        "enabled": True,
                    }
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        res = await client.patch(
            "/api/schedules/00000000-0000-0000-0000-000000000002",
            headers={"Authorization": f"Bearer {token}"},
            json={"enabled": False},
        )

    assert res.status_code == 200, res.text
    assert res.json()["enabled"] is False
    persisted = json.loads(schedule_path.read_text(encoding="utf-8"))
    assert persisted[0]["enabled"] is False


async def test_conversation_isolation_and_session_key(tmp_path):
    executor = FakeExecutor()
    app = make_app(tmp_path, executor)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        token_a = await register(client, "a@example.com")
        token_b = await register(client, "b@example.com")
        conv_a = await create_conversation(client, token_a)

        forbidden = await client.get(
            f"/api/conversations/{conv_a}/messages",
            headers={"Authorization": f"Bearer {token_b}"},
        )
        assert forbidden.status_code == 404

        post = await client.post(
            f"/api/conversations/{conv_a}/messages",
            headers={"Authorization": f"Bearer {token_a}"},
            json={"content": "hello"},
        )
        assert post.status_code == 200, post.text
        body = post.json()
        await wait_for_executor_call(executor)
        user_id = executor.calls[0]["user_id"]
        assert body["session_key"] == web_session_key(user_id, conv_a)
        assert executor.calls[0]["turn_id"] == body["turn_id"]


async def test_turn_stream_done_and_returns_pending_user_message(tmp_path):
    app = make_app(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        timeout=5.0,
    ) as client:
        token = await register(client, "stream@example.com")
        conv = await create_conversation(client, token)
        post = await client.post(
            f"/api/conversations/{conv}/messages",
            headers={"Authorization": f"Bearer {token}"},
            json={"content": "hello stream"},
        )
        turn_id = post.json()["turn_id"]
        stream = await client.get(
            f"/api/turns/{turn_id}/stream",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert stream.status_code == 200
        assert "event: content_delta" in stream.text
        assert "event: done" in stream.text
        assert post.json()["message"]["role"] == "user"
        assert post.json()["message"]["metadata"] == {"pending": True}

        messages = await client.get(
            f"/api/conversations/{conv}/messages",
            headers={"Authorization": f"Bearer {token}"},
        )
        roles = [item["role"] for item in messages.json()]
        assert roles == []


async def test_turn_stream_forwards_thinking_and_content_deltas(tmp_path):
    app = make_app(tmp_path, StreamingFakeExecutor())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        timeout=5.0,
    ) as client:
        token = await register(client, "stream-delta@example.com")
        conv = await create_conversation(client, token)
        post = await client.post(
            f"/api/conversations/{conv}/messages",
            headers={"Authorization": f"Bearer {token}"},
            json={"content": "hello stream"},
        )
        turn_id = post.json()["turn_id"]
        stream = await client.get(
            f"/api/turns/{turn_id}/stream",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert stream.status_code == 200
    assert "event: thinking_delta" in stream.text
    assert "先想一下" in stream.text
    assert stream.text.count("event: content_delta") == 2
    assert "你好" in stream.text
    assert "呀" in stream.text
    assert "event: done" in stream.text


async def test_agent_failure_streams_error(tmp_path):
    app = make_app(tmp_path, FakeExecutor(fail=True))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        timeout=5.0,
    ) as client:
        token = await register(client, "fail@example.com")
        conv = await create_conversation(client, token)
        post = await client.post(
            f"/api/conversations/{conv}/messages",
            headers={"Authorization": f"Bearer {token}"},
            json={"content": "break"},
        )
        turn_id = post.json()["turn_id"]
        stream = await client.get(
            f"/api/turns/{turn_id}/stream",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert stream.status_code == 200
        assert "event: error" in stream.text
        assert "agent failed" in stream.text


async def test_rate_limit_returns_429(tmp_path):
    app = make_app(tmp_path, FakeExecutor(delay=0.1))
    app.state.rate_limiter.max_per_minute = 1
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        token = await register(client, "limit@example.com")
        conv = await create_conversation(client, token)
        first = await client.post(
            f"/api/conversations/{conv}/messages",
            headers={"Authorization": f"Bearer {token}"},
            json={"content": "one"},
        )
        assert first.status_code == 200
        second = await client.post(
            f"/api/conversations/{conv}/messages",
            headers={"Authorization": f"Bearer {token}"},
            json={"content": "two"},
        )
        assert second.status_code == 429
