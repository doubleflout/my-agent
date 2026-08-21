"""淘汰策略：定期清理过期的 observe 数据。

规则：
  turns:        保留 180 天（error IS NOT NULL 永久保留）
  rag_queries:  保留  90 天（error IS NOT NULL 永久保留）

触发：启动时后台跑一次，距上次清理超过 24h 才执行。
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from .db import open_db
try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]

logger = logging.getLogger("observe.retention")

_RETENTION_DAYS = {
    "turns": 180,
    "rag_queries": 90,
}
_STAMP_FILE = ".last_cleanup"


def _stamp_path(db_path: Path) -> Path:
    return db_path.parent / _STAMP_FILE


def _should_run(db_path: Path) -> bool:
    stamp = _stamp_path(db_path)
    if not stamp.exists():
        return True
    import time

    age_hours = (time.time() - stamp.stat().st_mtime) / 3600
    return age_hours >= 24


def _run_cleanup(db_path: Path, database_url: str | None = None) -> None:
    conn = _open_cleanup_db(db_path, database_url)
    try:
        deleted: dict[str, int] = {}
        if _is_postgres_conn(conn):
            for table, days in _RETENTION_DAYS.items():
                cur = conn.execute(
                    f"DELETE FROM {table} WHERE ts < now() - (%s * interval '1 day') AND error IS NULL",
                    (days,),
                )
                deleted[table] = cur.rowcount
            conn.commit()
        else:
            with conn:
                for table, days in _RETENTION_DAYS.items():
                    cutoff = f"datetime('now', '-{days} days')"
                    cur = conn.execute(
                        f"DELETE FROM {table} WHERE ts < {cutoff} AND error IS NULL"
                    )
                    deleted[table] = cur.rowcount

        logger.info("observe retention done: %s", deleted)
        _ = _stamp_path(db_path).write_text("ok")
    except Exception:
        logger.exception("observe retention failed")
    finally:
        conn.close()


def _open_cleanup_db(db_path: Path, database_url: str | None) -> object:
    if database_url:
        if psycopg is None:
            raise RuntimeError("psycopg is required for postgres observe retention")
        return psycopg.connect(database_url)
    return open_db(db_path)


def _is_postgres_conn(conn: object) -> bool:
    if psycopg is None:
        return False
    return isinstance(conn, psycopg.Connection)


async def run_retention_if_needed(db_path: Path, *, database_url: str | None = None) -> None:
    """在 asyncio 后台跑清理（用 run_in_executor 避免阻塞事件循环）。"""
    if not db_path.exists():
        return
    if not _should_run(db_path):
        return
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _run_cleanup, db_path, database_url)
