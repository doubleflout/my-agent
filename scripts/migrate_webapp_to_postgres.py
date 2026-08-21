from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

try:
    import psycopg
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: psycopg. Install with `python -m pip install psycopg[binary]`."
    ) from exc

from scripts.migrate_sqlite_to_postgres import ensure_schema, migrate_webapp


DEFAULT_DATABASE_URL = "postgresql://postgres:postgres123@localhost:5432/akashic_agent"


def print_counts(counts: dict[str, int]) -> None:
    if not counts:
        print("webapp: no rows migrated")
        return
    print("webapp:")
    for key in sorted(counts):
        print(f"  {key}: {counts[key]}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate only workspace/webapp.db into the shared PostgreSQL database."
    )
    parser.add_argument("--workspace", type=Path, default=Path.home() / ".akashic" / "workspace")
    parser.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    parser.add_argument("--schema", type=Path, default=Path(__file__).with_name("postgres_schema.sql"))
    parser.add_argument("--skip-schema", action="store_true")
    args = parser.parse_args()

    with psycopg.connect(args.database_url) as pg:
        if not args.skip_schema:
            ensure_schema(pg, args.schema)
        counts = migrate_webapp(pg, workspace=args.workspace)
        pg.commit()
    print_counts(counts)


if __name__ == "__main__":
    main()
