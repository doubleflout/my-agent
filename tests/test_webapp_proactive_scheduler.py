from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from webapp.proactive_scheduler import WebProactiveJob, WebProactiveScheduler
from webapp.store import ProactiveSessionRecord


class _Config:
    proactive = object()


class _Store:
    def __init__(self) -> None:
        self.messages: list[dict] = []
        self.scheduled: list[str] = []
        self.record = ProactiveSessionRecord(
            id="ps-1",
            user_id="user-1",
            conversation_id="conversation-1",
            session_key="web:proactive:user-1:conversation-1",
            enabled=True,
            last_tick_at=None,
            next_tick_at=datetime(2026, 8, 9, 8, 0, tzinfo=timezone.utc),
            interval_seconds=4800,
            created_at=datetime(2026, 8, 9, 7, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 8, 9, 7, 0, tzinfo=timezone.utc),
        )

    def list_due_proactive_sessions(self, *, now, limit):
        return [self.record]

    def add_message(self, **kwargs):
        self.messages.append(kwargs)

    def schedule_next_proactive_tick(self, *, session_key, cfg, now):
        self.scheduled.append(session_key)
        return 4800


class _Runner:
    def __init__(self, content: str | None) -> None:
        self.content = content
        self.jobs: list[WebProactiveJob] = []

    async def run(self, job: WebProactiveJob) -> str | None:
        self.jobs.append(job)
        return self.content


def test_scheduler_runs_due_session_and_writes_message() -> None:
    asyncio.run(_run_scheduler_runs_due_session_and_writes_message())


async def _run_scheduler_runs_due_session_and_writes_message() -> None:
    store = _Store()
    runner = _Runner("主动问候")
    scheduler = WebProactiveScheduler(
        store=store,  # type: ignore[arg-type]
        config=_Config(),  # type: ignore[arg-type]
        runner=runner,
    )

    count = await scheduler.run_once(datetime(2026, 8, 9, 8, 0, tzinfo=timezone.utc))

    assert count == 1
    assert runner.jobs[0].session_key == "web:proactive:user-1:conversation-1"
    assert store.messages[0]["metadata"]["source"] == "proactive"
    assert store.scheduled == ["web:proactive:user-1:conversation-1"]


def test_scheduler_reschedules_when_runner_skips_message() -> None:
    asyncio.run(_run_scheduler_reschedules_when_runner_skips_message())


async def _run_scheduler_reschedules_when_runner_skips_message() -> None:
    store = _Store()
    runner = _Runner(None)
    scheduler = WebProactiveScheduler(
        store=store,  # type: ignore[arg-type]
        config=_Config(),  # type: ignore[arg-type]
        runner=runner,
    )

    count = await scheduler.run_once(datetime(2026, 8, 9, 8, 0, tzinfo=timezone.utc))

    assert count == 1
    assert store.messages == []
    assert store.scheduled == ["web:proactive:user-1:conversation-1"]
