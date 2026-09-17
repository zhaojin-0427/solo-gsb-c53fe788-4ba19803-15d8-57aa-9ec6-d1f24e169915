"""Connection pool management and schema initialisation."""
import asyncio
from pathlib import Path

import asyncpg

SCHEMA_PATH = Path(__file__).parent / "db" / "schema.sql"

_pool: asyncpg.Pool | None = None


def split_sql(sql: str) -> list[str]:
    """Split a SQL script at top-level semicolons.

    asyncpg's ``execute`` runs one statement at a time, so the schema
    script (which contains dollar-quoted function bodies, comments and
    string literals with embedded semicolons) must be split with a small
    tokenizer instead of ``sql.split(';')``.
    """
    statements: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql)

    while i < n:
        c = sql[i]

        # Line comment
        if c == "-" and sql[i : i + 2] == "--":
            j = sql.find("\n", i)
            j = n if j == -1 else j + 1
            buf.append(sql[i:j])
            i = j
            continue

        # Block comment (nested comments are possible in Postgres)
        if c == "/" and sql[i : i + 2] == "/*":
            depth, j = 1, i + 2
            while j < n and depth:
                if sql[j : j + 2] == "/*":
                    depth += 1
                    j += 2
                elif sql[j : j + 2] == "*/":
                    depth -= 1
                    j += 2
                else:
                    j += 1
            buf.append(sql[i:j])
            i = j
            continue

        # Single-quoted string with '' escaping
        if c == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if sql[j : j + 2] == "''":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            buf.append(sql[i:j])
            i = j
            continue

        # Quoted identifier
        if c == '"':
            j = i + 1
            while j < n:
                if sql[j] == '"':
                    if sql[j : j + 2] == '""':
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            buf.append(sql[i:j])
            i = j
            continue

        # Dollar-quoted string / function body: $tag$ ... $tag$
        # (the tag may be empty, i.e. a plain $$ ... $$ body).
        if c == "$":
            j = i + 1
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            if j < n and sql[j] == "$":
                tag = sql[i : j + 1]
                end = sql.find(tag, j + 1)
                if end != -1:
                    buf.append(sql[i : end + len(tag)])
                    i = end + len(tag)
                    continue
            buf.append(c)
            i += 1
            continue

        if c == ";":
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(c)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


async def init_pool(dsn: str, *, min_size: int, max_size: int,
                    timeout: float) -> asyncpg.Pool:
    global _pool
    last_error: Exception | None = None
    # Postgres in the compose stack may still be starting; retry.
    for attempt in range(max(1, int(timeout / 2))):
        try:
            _pool = await asyncpg.create_pool(
                dsn, min_size=min_size, max_size=max_size,
                timeout=timeout, max_inactive_connection_lifetime=300)
            break
        except (OSError, asyncpg.PostgresError) as exc:
            last_error = exc
            if attempt + 1 >= int(timeout / 2):
                break
            await asyncio.sleep(2)
    if _pool is None:  # pragma: no cover - startup failure path
        raise RuntimeError(f"cannot connect to database: {last_error}")

    async with _pool.acquire() as conn:
        for stmt in split_sql(SCHEMA_PATH.read_text()):
            await conn.execute(stmt)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("database pool is not initialised")
    return _pool
