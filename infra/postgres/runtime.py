from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

from .settings import PostgresSettings


POSTGRES_DIR = Path(__file__).resolve().parent
DOCKER_COMPOSE_FILE = POSTGRES_DIR / "docker-compose.yml"
DOCKER_COMPOSE_SERVICE = "topo-postgres"


def _require_psycopg() -> object:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL support requires the 'psycopg' package. "
            "Install dependencies from requirements.txt before running with --storage postgres."
        ) from exc
    return psycopg


def _check_sql_connection(settings: PostgresSettings) -> None:
    psycopg = _require_psycopg()
    with psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.database,
        user=settings.user,
        password=settings.password,
        connect_timeout=settings.connect_timeout,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()


def _run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=POSTGRES_DIR,
        text=True,
        capture_output=True,
        check=False,
    )


def _detect_compose_command() -> list[str]:
    docker = shutil.which("docker")
    if docker:
        result = _run_command([docker, "compose", "version"])
        if result.returncode == 0:
            return [docker, "compose"]

    docker_compose = shutil.which("docker-compose")
    if docker_compose:
        result = _run_command([docker_compose, "version"])
        if result.returncode == 0:
            return [docker_compose]

    raise RuntimeError(
        "PostgreSQL is unavailable and Docker Compose was not found. "
        "Install Docker Desktop or start PostgreSQL manually."
    )


def _run_compose(compose_command: list[str], *args: str) -> subprocess.CompletedProcess[str]:
    return _run_command([*compose_command, "-f", str(DOCKER_COMPOSE_FILE), *args])


def _is_service_running(compose_command: list[str]) -> bool:
    result = _run_compose(
        compose_command,
        "ps",
        "--status",
        "running",
        "--services",
        DOCKER_COMPOSE_SERVICE,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"Failed to inspect PostgreSQL container: {stderr}")
    services = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    return DOCKER_COMPOSE_SERVICE in services


def _start_service(compose_command: list[str]) -> None:
    result = _run_compose(compose_command, "up", "-d", DOCKER_COMPOSE_SERVICE)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            "Failed to start PostgreSQL via Docker Compose. "
            f"Compose file: {DOCKER_COMPOSE_FILE}. Details: {stderr}"
        )


def _wait_for_postgres(settings: PostgresSettings, timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            _check_sql_connection(settings)
            return
        except Exception as exc:  # pragma: no cover - exercised through public API
            last_error = exc
            time.sleep(2)

    message = (
        f"PostgreSQL did not become ready within {timeout_seconds}s "
        f"at {settings.host}:{settings.port}/{settings.database}."
    )
    if last_error is not None:
        message = f"{message} Last error: {last_error}"
    raise RuntimeError(message)


def ensure_postgres_ready(
    settings: PostgresSettings,
    auto_start: bool = True,
    timeout_seconds: int = 90,
) -> None:
    try:
        _check_sql_connection(settings)
        return
    except Exception as initial_error:
        if not auto_start:
            raise RuntimeError(
                "PostgreSQL is not reachable and auto-start is disabled. "
                f"Tried {settings.host}:{settings.port}/{settings.database}. "
                f"Original error: {initial_error}"
            ) from initial_error

    compose_command = _detect_compose_command()

    if not _is_service_running(compose_command):
        _start_service(compose_command)

    _wait_for_postgres(settings, timeout_seconds)
