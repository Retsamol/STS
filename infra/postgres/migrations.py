from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from .settings import PostgresSettings, load_postgres_settings


POSTGRES_SQL_DIR = Path(__file__).resolve().parents[2] / "db" / "postgres"
MIGRATION_SQL_DIR = POSTGRES_SQL_DIR / "migrations"
MIGRATION_SCHEMA = "topo"
MIGRATION_TABLE = "schema_migration"


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Migration:
    version: str
    name: str
    sql_paths: tuple[Path, ...]

    @property
    def checksum_sha256(self) -> str:
        return hashlib.sha256(_migration_payload(self).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class AppliedMigration:
    version: str
    name: str
    checksum_sha256: str


@dataclass(frozen=True, slots=True)
class MigrationResult:
    version: str
    name: str
    applied: bool
    checksum_sha256: str


MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        version="0001_core_schema",
        name="Core topo and legacy inventory schema",
        sql_paths=(POSTGRES_SQL_DIR / "schema.sql",),
    ),
    Migration(
        version="0002_v2_and_explicit_schema",
        name="Topo v2, inventory v2, and explicit scenario schema",
        sql_paths=(
            POSTGRES_SQL_DIR / "schema_v2.sql",
            POSTGRES_SQL_DIR / "explicit_scenario_schema.sql",
        ),
    ),
    Migration(
        version="0003_topo_export_runtime_delta",
        name="Promote topo export runtime DDL to managed migration",
        sql_paths=(MIGRATION_SQL_DIR / "0003_topo_export_runtime_delta.sql",),
    ),
    Migration(
        version="0004_explicit_resource_limit_runtime_fields",
        name="Add explicit resource limit runtime fields",
        sql_paths=(MIGRATION_SQL_DIR / "0004_explicit_resource_limit_runtime_fields.sql",),
    ),
)


def _require_psycopg() -> object:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL migrations require the 'psycopg' package. "
            "Install dependencies from requirements.txt before applying migrations."
        ) from exc
    return psycopg


def _connection_kwargs(settings: PostgresSettings) -> dict[str, Any]:
    return {
        "host": settings.host,
        "port": settings.port,
        "dbname": settings.database,
        "user": settings.user,
        "password": settings.password,
        "connect_timeout": settings.connect_timeout,
    }


def _read_sql(path: Path) -> str:
    if not path.exists():
        raise MigrationError(f"Migration SQL file is missing: {path}")
    return path.read_text(encoding="utf-8").replace("\r\n", "\n")


def _migration_payload(migration: Migration) -> str:
    parts: list[str] = []
    for path in migration.sql_paths:
        parts.append(f"-- {path.relative_to(POSTGRES_SQL_DIR)}\n{_read_sql(path)}")
    return "\n\n".join(parts)


def _ordered_migrations(
    migrations: Sequence[Migration] = MIGRATIONS,
    *,
    target_version: str | None = None,
) -> tuple[Migration, ...]:
    versions = [migration.version for migration in migrations]
    if len(versions) != len(set(versions)):
        raise MigrationError("Duplicate PostgreSQL migration version in registry.")
    if target_version is None:
        return tuple(migrations)
    if target_version not in versions:
        raise MigrationError(f"Unknown PostgreSQL migration target: {target_version}")
    return tuple(migrations[: versions.index(target_version) + 1])


def plan_pending_migrations(
    applied_versions: Iterable[str],
    migrations: Sequence[Migration] = MIGRATIONS,
    *,
    target_version: str | None = None,
) -> tuple[Migration, ...]:
    applied = {str(version) for version in applied_versions}
    known = {migration.version for migration in migrations}
    unknown = sorted(applied - known)
    if unknown:
        raise MigrationError(f"Database has unknown PostgreSQL migrations: {', '.join(unknown)}")
    return tuple(migration for migration in _ordered_migrations(migrations, target_version=target_version) if migration.version not in applied)


def _ensure_registry(cursor: Any) -> None:
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {MIGRATION_SCHEMA}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MIGRATION_SCHEMA}.{MIGRATION_TABLE} (
          version text PRIMARY KEY,
          name text NOT NULL,
          checksum_sha256 text NOT NULL,
          applied_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )


def _load_applied(cursor: Any) -> dict[str, AppliedMigration]:
    cursor.execute(
        f"""
        SELECT version, name, checksum_sha256
        FROM {MIGRATION_SCHEMA}.{MIGRATION_TABLE}
        ORDER BY version
        """
    )
    return {
        str(row[0]): AppliedMigration(
            version=str(row[0]),
            name=str(row[1]),
            checksum_sha256=str(row[2]),
        )
        for row in cursor.fetchall()
    }


def _validate_applied_checksums(applied: dict[str, AppliedMigration], migrations: Sequence[Migration]) -> None:
    by_version = {migration.version: migration for migration in migrations}
    unknown = sorted(set(applied) - set(by_version))
    if unknown:
        raise MigrationError(f"Database has unknown PostgreSQL migrations: {', '.join(unknown)}")
    for version, record in applied.items():
        expected = by_version[version].checksum_sha256
        if record.checksum_sha256 != expected:
            raise MigrationError(
                f"PostgreSQL migration checksum mismatch for {version}: "
                f"database={record.checksum_sha256}, code={expected}"
            )


def apply_migrations(
    postgres: PostgresSettings | None = None,
    *,
    target_version: str | None = None,
    migrations: Sequence[Migration] = MIGRATIONS,
) -> tuple[MigrationResult, ...]:
    postgres = postgres or load_postgres_settings()
    ordered = _ordered_migrations(migrations, target_version=target_version)
    psycopg = _require_psycopg()
    results: list[MigrationResult] = []

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            _ensure_registry(cursor)
            applied = _load_applied(cursor)
            _validate_applied_checksums(applied, migrations)
            pending = plan_pending_migrations(applied, migrations, target_version=target_version)
            pending_versions = {migration.version for migration in pending}

            for migration in ordered:
                checksum = migration.checksum_sha256
                if migration.version not in pending_versions:
                    results.append(
                        MigrationResult(
                            version=migration.version,
                            name=migration.name,
                            applied=False,
                            checksum_sha256=checksum,
                        )
                    )
                    continue
                for sql_path in migration.sql_paths:
                    cursor.execute(_read_sql(sql_path))
                cursor.execute(
                    f"""
                    INSERT INTO {MIGRATION_SCHEMA}.{MIGRATION_TABLE}
                      (version, name, checksum_sha256)
                    VALUES (%s, %s, %s)
                    """,
                    (migration.version, migration.name, checksum),
                )
                results.append(
                    MigrationResult(
                        version=migration.version,
                        name=migration.name,
                        applied=True,
                        checksum_sha256=checksum,
                    )
                )
        connection.commit()

    return tuple(results)
