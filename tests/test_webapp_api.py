from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx

from webapp.agent_executor import web_session_key
from webapp.app import create_web_app
from webapp.store import WebStore


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
    ) -> str:
        self.calls.append(
            {
                "content": content,
                "user_id": user_id,
                "conversation_id": conversation_id,
                "session_key": session_key or "",
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
        on_stream_event=None,
    ) -> str:
        self.calls.append(
            {
                "content": content,
                "user_id": user_id,
                "conversation_id": conversation_id,
                "session_key": session_key or "",
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
