"""
One-shot init script run inside Docker before app services start.
  1. Applies infrastructure/postgres/schema.sql
  2. Creates all Kafka topics (idempotent)
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


async def apply_schema() -> None:
    import asyncpg

    url = os.environ["POSTGRES_URL"].replace("postgresql+asyncpg://", "postgresql://")
    sql = (ROOT / "infrastructure" / "postgres" / "schema.sql").read_text()
    conn = await asyncpg.connect(url)
    try:
        await conn.execute(sql)
        print("[init] DB schema applied.")
    finally:
        await conn.close()


async def create_topics() -> None:
    from infrastructure.kafka.create_topics import create_all_topics
    await create_all_topics()


async def main() -> None:
    print("[init] Applying DB schema ...")
    await apply_schema()
    print("[init] Creating Kafka topics ...")
    await create_topics()
    print("[init] Done.")


if __name__ == "__main__":
    asyncio.run(main())
