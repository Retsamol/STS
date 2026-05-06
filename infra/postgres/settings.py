from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


POSTGRES_DIR = Path(__file__).resolve().parent


@dataclass(slots=True)
class PostgresSettings:
    host: str = "127.0.0.1"
    port: int = 5432
    database: str = "topo"
    user: str = "postgres"
    password: str = "postgres"
    schema: str = "topo"
    connect_timeout: int = 5


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def load_postgres_settings() -> PostgresSettings:
    example_env = _read_env_file(POSTGRES_DIR / ".env.example")
    file_env = _read_env_file(POSTGRES_DIR / ".env")

    env = {**example_env, **file_env, **os.environ}
    return PostgresSettings(
        host=env.get("POSTGRES_HOST", "127.0.0.1"),
        port=int(env.get("POSTGRES_PORT", "5432")),
        database=env.get("POSTGRES_DB", "topo"),
        user=env.get("POSTGRES_USER", "postgres"),
        password=env.get("POSTGRES_PASSWORD", "postgres"),
        schema=env.get("POSTGRES_SCHEMA", "topo"),
        connect_timeout=int(env.get("POSTGRES_CONNECT_TIMEOUT", "5")),
    )
