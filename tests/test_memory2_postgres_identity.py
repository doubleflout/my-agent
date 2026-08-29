from __future__ import annotations

from pathlib import Path

import pytest

from memory2 import postgres_store


class _FakeConn:
    def execute(self, *_args, **_kwargs):
        return self

    def fetchone(self):
        return None

    def close(self) -> None:
        pass


def test_resolve_memory_user_id_raises_when_workspace_has_no_user_mapping(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    monkeypatch.setattr(postgres_store.psycopg, "connect", lambda *_args, **_kwargs: _FakeConn())

    with pytest.raises(RuntimeError, match="cannot resolve memory user_id"):
        postgres_store.resolve_memory_user_id(
            workspace=tmp_path / "workspace",
            database_url="postgresql://postgres:postgres123@localhost:5432/akashic_agent",
        )


def test_resolve_memory_user_id_uses_user_workspace_directory(tmp_path: Path):
    workspace = tmp_path / "workspace" / "users" / "user-1"

    assert (
        postgres_store.resolve_memory_user_id(
            workspace=workspace,
            database_url="postgresql://postgres:postgres123@localhost:5432/akashic_agent",
        )
        == "user-1"
    )
