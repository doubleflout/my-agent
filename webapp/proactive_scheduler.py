from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

from agent.config_models import Config
from webapp.store import ProactiveSessionRecord, WebStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WebProactiveJob:
    user_id: str
    conversation_id: str
    session_key: str
    due_at: datetime


class WebProactiveRunner(Protocol):
    async def run(self, job: WebProactiveJob) -> str | None:
        ...


class WebProactiveScheduler:
    def __init__(
        self,
        *,
        store: WebStore,
        config: Config,
        runner: WebProactiveRunner,
        poll_interval_seconds: int = 30,
        batch_size: int = 50,
        max_concurrency: int = 4,
    ) -> None:
        self._store = store
        self._config = config
        self._runner = runner
        self._poll_interval_seconds = max(1, int(poll_interval_seconds))
        self._batch_size = max(1, int(batch_size))
        self._sem = asyncio.Semaphore(max(1, int(max_concurrency)))
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info("[web.proactive] scheduler started")
        try:
            while self._running:
                await self.run_once()
                await asyncio.sleep(self._poll_interval_seconds)
        finally:
            logger.info("[web.proactive] scheduler stopped")

    def stop(self) -> None:
        self._running = False

    async def run_once(self, now: datetime | None = None) -> int:
        tick_now = now or datetime.now(timezone.utc)
        due = self._store.list_due_proactive_sessions(
            now=tick_now,
            limit=self._batch_size,
        )
        if not due:
            return 0
        await asyncio.gather(*(self._run_record(record, tick_now) for record in due))
        return len(due)

    async def _run_record(
        self,
        record: ProactiveSessionRecord,
        now: datetime,
    ) -> None:
        async with self._sem:
            job = WebProactiveJob(
                user_id=record.user_id,
                conversation_id=record.conversation_id,
                session_key=record.session_key,
                due_at=record.next_tick_at,
            )
            logger.info(
                "[web.proactive] entering proactive session user=%s conversation=%s session=%s due_at=%s",
                job.user_id,
                job.conversation_id,
                job.session_key,
                job.due_at.isoformat(),
            )
            try:
                content = await self._runner.run(job)
                if content:
                    self._store.add_message(
                        conversation_id=record.conversation_id,
                        user_id=record.user_id,
                        role="assistant",
                        content=content,
                        metadata={
                            "source": "proactive",
                            "session_key": record.session_key,
                            "due_at": record.next_tick_at.isoformat(),
                        },
                    )
            except Exception:
                logger.exception(
                    "[web.proactive] job failed session=%s user=%s",
                    record.session_key,
                    record.user_id,
                )
            finally:
                interval = self._store.schedule_next_proactive_tick(
                    session_key=record.session_key,
                    cfg=self._config.proactive,
                    now=now,
                )
                next_tick_at = now + timedelta(seconds=interval)
                logger.info(
                    "[web.proactive] scheduled next tick user=%s conversation=%s session=%s interval=%ss next_tick_at=%s",
                    record.user_id,
                    record.conversation_id,
                    record.session_key,
                    interval,
                    next_tick_at.isoformat(),
                )
