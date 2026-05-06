from __future__ import annotations

from typing import Any

from .inventory_store import INVENTORY_SCHEMA
from .settings import PostgresSettings, load_postgres_settings


def _require_psycopg() -> object:
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise RuntimeError(
            "Inventory PostgreSQL support requires the 'psycopg' package. "
            "Install dependencies from requirements.txt before seeding inventory."
        ) from exc
    return psycopg, Jsonb


def _connection_kwargs(settings: PostgresSettings) -> dict[str, Any]:
    return {
        "host": settings.host,
        "port": settings.port,
        "dbname": settings.database,
        "user": settings.user,
        "password": settings.password,
        "connect_timeout": settings.connect_timeout,
    }


def _base_legacy_inputs():
    from simulation_core import scenario as scenario_module

    return scenario_module.ScenarioInputs(
        cgs_json=scenario_module.CONFIGS_OLD_DIR / "cgs.json",
        cgs_key="1",
        targets_json=scenario_module.CONFIGS_OLD_DIR / "hubs.json",
        targets_key="101",
        sat_json=scenario_module.CONFIGS_OLD_DIR / "sat.json",
        sat_key="test",
        haps_json=scenario_module.CONFIGS_OLD_DIR / "HAPS.json",
        haps_key="5",
        vsat_db=scenario_module.CONFIGS_OLD_DIR / "vsat_payload.db",
    )


def ensure_inventory_schema(postgres: PostgresSettings | None = None) -> None:
    postgres = postgres or load_postgres_settings()
    from .migrations import apply_migrations

    apply_migrations(postgres, target_version="0001_core_schema")


def seed_inventory_from_legacy(postgres: PostgresSettings | None = None, *, vsat_set: str = "default") -> None:
    from dataclasses import replace

    from simulation_core import scenario as scenario_module

    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg()
    ensure_inventory_schema(postgres)
    defaults = scenario_module._defaults_from_raw(None, legacy_mode=True)
    options = scenario_module.available_config_options()
    base_inputs = _base_legacy_inputs()
    schema = INVENTORY_SCHEMA

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            for set_key in options["cgs"]:
                inputs = replace(base_inputs, cgs_key=str(set_key))
                for ordinal, item in enumerate(scenario_module._legacy_gateways(inputs, defaults)):
                    cursor.execute(
                        f"""
                        INSERT INTO {schema}.gateway_node (
                            set_key, external_id, name, lat, lon, site_alt_m, antenna_height_agl_m,
                            radio_profile, role, limits, metadata, ordinal
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (set_key, external_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon,
                            site_alt_m = EXCLUDED.site_alt_m,
                            antenna_height_agl_m = EXCLUDED.antenna_height_agl_m,
                            radio_profile = EXCLUDED.radio_profile,
                            role = EXCLUDED.role,
                            limits = EXCLUDED.limits,
                            metadata = EXCLUDED.metadata,
                            ordinal = EXCLUDED.ordinal
                        """,
                        (
                            str(set_key),
                            str(item.external_id),
                            str(item.name),
                            float(item.lat),
                            float(item.lon),
                            item.site_alt_m,
                            float(item.antenna_height_agl_m),
                            str(item.radio_profile),
                            str(item.role),
                            Jsonb(dict(item.limits)),
                            Jsonb(dict(item.metadata)),
                            int(ordinal),
                        ),
                    )

            for set_key in options["targets"]:
                inputs = replace(base_inputs, targets_key=str(set_key))
                for ordinal, item in enumerate(scenario_module._legacy_targets(inputs, defaults)):
                    cursor.execute(
                        f"""
                        INSERT INTO {schema}.target_hub (
                            set_key, external_id, name, lat, lon, frequency, priority,
                            site_alt_m, antenna_height_agl_m, metadata, ordinal
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (set_key, external_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon,
                            frequency = EXCLUDED.frequency,
                            priority = EXCLUDED.priority,
                            site_alt_m = EXCLUDED.site_alt_m,
                            antenna_height_agl_m = EXCLUDED.antenna_height_agl_m,
                            metadata = EXCLUDED.metadata,
                            ordinal = EXCLUDED.ordinal
                        """,
                        (
                            str(set_key),
                            str(item.external_id),
                            str(item.name),
                            float(item.lat),
                            float(item.lon),
                            int(item.frequency),
                            float(item.priority),
                            item.site_alt_m,
                            float(item.antenna_height_agl_m),
                            Jsonb(dict(item.metadata)),
                            int(ordinal),
                        ),
                    )

            for set_key in options["sat"]:
                inputs = replace(base_inputs, sat_key=str(set_key))
                for ordinal, item in enumerate(scenario_module._legacy_satellites(inputs)):
                    cursor.execute(
                        f"""
                        INSERT INTO {schema}.satellite_node (
                            set_key, external_id, name, tle_line1, tle_line2, radio_profile,
                            connection_min, beam_layout_mode, dynamic_ray_count, dynamic_ray_aperture_deg,
                            sat_haps_ray_count, resource_limits, allowed_link_types, metadata, ordinal
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (set_key, external_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            tle_line1 = EXCLUDED.tle_line1,
                            tle_line2 = EXCLUDED.tle_line2,
                            radio_profile = EXCLUDED.radio_profile,
                            connection_min = EXCLUDED.connection_min,
                            beam_layout_mode = EXCLUDED.beam_layout_mode,
                            dynamic_ray_count = EXCLUDED.dynamic_ray_count,
                            dynamic_ray_aperture_deg = EXCLUDED.dynamic_ray_aperture_deg,
                            sat_haps_ray_count = EXCLUDED.sat_haps_ray_count,
                            resource_limits = EXCLUDED.resource_limits,
                            allowed_link_types = EXCLUDED.allowed_link_types,
                            metadata = EXCLUDED.metadata,
                            ordinal = EXCLUDED.ordinal
                        """,
                        (
                            str(set_key),
                            str(item.external_id),
                            str(item.name),
                            str(item.tle_line1),
                            str(item.tle_line2),
                            str(item.radio_profile),
                            int(item.connection_min),
                            str(item.beam_layout_mode),
                            int(item.dynamic_ray_count),
                            float(item.dynamic_ray_aperture_deg),
                            int(item.sat_haps_ray_count),
                            Jsonb(dict(item.resource_limits)),
                            Jsonb(list(item.allowed_link_types)),
                            Jsonb(dict(item.metadata)),
                            int(ordinal),
                        ),
                    )

            for set_key in options["haps"]:
                inputs = replace(base_inputs, haps_key=str(set_key))
                for ordinal, item in enumerate(scenario_module._legacy_haps(inputs)):
                    cursor.execute(
                        f"""
                        INSERT INTO {schema}.haps_node (
                            set_key, external_id, name, lat, lon, alt_m, radio_profile,
                            connection_min, resource_limits, metadata, ordinal
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (set_key, external_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon,
                            alt_m = EXCLUDED.alt_m,
                            radio_profile = EXCLUDED.radio_profile,
                            connection_min = EXCLUDED.connection_min,
                            resource_limits = EXCLUDED.resource_limits,
                            metadata = EXCLUDED.metadata,
                            ordinal = EXCLUDED.ordinal
                        """,
                        (
                            str(set_key),
                            str(item.external_id),
                            str(item.name),
                            float(item.lat),
                            float(item.lon),
                            float(item.alt_m),
                            str(item.radio_profile),
                            int(item.connection_min),
                            Jsonb(dict(item.resource_limits)),
                            Jsonb(dict(item.metadata)),
                            int(ordinal),
                        ),
                    )

            for ordinal, item in enumerate(scenario_module._legacy_vsats(base_inputs, defaults)):
                cursor.execute(
                    f"""
                    INSERT INTO {schema}.vsat_node (
                        set_key, external_id, name, lat, lon, region_code, site_alt_m,
                        antenna_height_agl_m, radio_profile, role, limits, metadata, ordinal
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (set_key, external_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        region_code = EXCLUDED.region_code,
                        site_alt_m = EXCLUDED.site_alt_m,
                        antenna_height_agl_m = EXCLUDED.antenna_height_agl_m,
                        radio_profile = EXCLUDED.radio_profile,
                        role = EXCLUDED.role,
                        limits = EXCLUDED.limits,
                        metadata = EXCLUDED.metadata,
                        ordinal = EXCLUDED.ordinal
                    """,
                    (
                        str(vsat_set),
                        str(item.external_id),
                        str(item.name),
                        float(item.lat),
                        float(item.lon),
                        int(item.region_code),
                        item.site_alt_m,
                        float(item.antenna_height_agl_m),
                        str(item.radio_profile),
                        str(item.role),
                        Jsonb(dict(item.limits)),
                        Jsonb(dict(item.metadata)),
                        int(ordinal),
                    ),
                )

        connection.commit()
