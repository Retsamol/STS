from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .settings import PostgresSettings, load_postgres_settings


INVENTORY_SCHEMA = "inventory"
INVENTORY_V2_SCHEMA = "inventory_v2"
EXPLICIT_SCHEMA = "inventory_explicit"
SUPPORTED_INVENTORY_SOURCES = {"postgres", "postgres_v2"}

_GROUND_EXPLICIT_FLOAT_FIELDS = (
    "tx_power_dbw",
    "tx_antenna_diameter_m",
    "tx_antenna_gain_dbi",
    "tx_center_frequency_ghz",
    "tx_bandwidth_mhz",
    "tx_polarization_deg",
    "tx_waveguide_loss_db",
    "rx_antenna_diameter_m",
    "rx_antenna_gain_dbi",
    "rx_center_frequency_ghz",
    "rx_bandwidth_mhz",
    "rx_polarization_deg",
    "rx_waveguide_loss_db",
    "lna_noise_temperature_k",
    "rolloff",
    "lm_db",
    "if_to_rf_degradation_db",
    "rain_probability_percent",
    "off_axis_loss_db_per_rad",
)
_GROUND_EXPLICIT_TEXT_FIELDS = ("antenna_pattern_reference", "source_name")
_RELAY_EXPLICIT_FLOAT_FIELDS = (
    "eirp_sat_dbw",
    "gt_dbk",
    "sfd_dbw_m2",
    "ibo_db",
    "obo_db",
    "npr_db",
    "tx_power_dbw",
    "tx_center_frequency_ghz",
    "tx_bandwidth_mhz",
    "tx_antenna_diameter_m",
    "tx_antenna_gain_dbi",
    "tx_polarization_deg",
    "tx_waveguide_loss_db",
    "rx_center_frequency_ghz",
    "rx_bandwidth_mhz",
    "rx_antenna_diameter_m",
    "rx_antenna_gain_dbi",
    "rx_polarization_deg",
    "rx_waveguide_loss_db",
    "rx_noise_temperature_k",
    "off_axis_loss_db_per_rad",
)
_RELAY_EXPLICIT_TEXT_FIELDS = ("antenna_pattern_reference", "source_name")
_RESOURCE_LIMIT_EXPLICIT_CONFIG = {
    "satellites": {
        "table": "satellite_resource_limit",
        "key_column": "satellite_key",
        "fields": ("max_user_links", "max_feeder_links", "max_interobject_links", "access_model"),
    },
    "haps": {
        "table": "haps_resource_limit",
        "key_column": "haps_key",
        "fields": (
            "max_user_links",
            "max_feeder_links",
            "max_haps_links",
            "angle_rad",
            "beam_angle_rad",
            "beam_angle_deg",
            "maxlength",
            "cgs_ray_count",
            "feeder_ray_count",
        ),
    },
}
_ALLOWED_LINK_EXPLICIT_CONFIG = {
    "satellites": {
        "table": "satellite_allowed_link_type",
        "key_column": "satellite_key",
    },
    "haps": {
        "table": "haps_allowed_link_type",
        "key_column": "haps_key",
    },
}


@dataclass(slots=True)
class InventoryBundle:
    gateways: list[dict[str, Any]]
    targets: list[dict[str, Any]]
    satellites: list[dict[str, Any]]
    haps: list[dict[str, Any]]
    vsats: list[dict[str, Any]]
    theoretical_subscribers: list[dict[str, Any]]
    ground_terminal_profiles: dict[str, dict[str, Any]]
    relay_payload_profiles: dict[str, dict[str, Any]]


def _require_psycopg() -> object:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "Inventory PostgreSQL support requires the 'psycopg' package. "
            "Install dependencies from requirements.txt before using inventory_db."
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


def _fetch_rows(cursor: Any, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cursor.execute(query, params)
    columns = [desc[0] for desc in cursor.description]
    rows: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        rows.append({str(columns[index]): row[index] for index in range(len(columns))})
    return rows


def _selection_value(selection: Any, kind: str, legacy_field: str) -> str:
    selector_for = getattr(selection, "selector_for", None)
    if callable(selector_for):
        resolved = selector_for(kind)
        if resolved is not None:
            return str(resolved)
    return str(getattr(selection, legacy_field))


def _selection_source(selection: Any) -> str:
    return str(getattr(selection, "source", "postgres") or "postgres")


def _relation_exists(cursor: Any, relation_name: str) -> bool:
    rows = _fetch_rows(cursor, "SELECT to_regclass(%s) AS relation", (relation_name,))
    return bool(rows and rows[0].get("relation") is not None)


def _load_ground_terminal_profiles_v2(cursor: Any) -> dict[str, dict[str, Any]]:
    legacy_profiles = _load_ground_terminal_profiles_legacy_v2(cursor)
    explicit_profiles = _load_ground_terminal_profiles_explicit(cursor)
    return {**legacy_profiles, **explicit_profiles}


def _load_ground_terminal_profiles_legacy_v2(cursor: Any) -> dict[str, dict[str, Any]]:
    rows = _fetch_rows(
        cursor,
        f"""
        SELECT profile_key, station_kind, params, metadata
        FROM {INVENTORY_V2_SCHEMA}.ground_terminal_profile
        ORDER BY profile_key
        """,
        (),
    )
    profiles: dict[str, dict[str, Any]] = {}
    for row in rows:
        params = dict(row["params"]) if isinstance(row.get("params"), dict) else {}
        metadata = dict(row["metadata"]) if isinstance(row.get("metadata"), dict) else {}
        profiles[str(row["profile_key"])] = {
            "station_kind": str(row["station_kind"]),
            **params,
            "metadata": metadata,
        }
    return profiles


def _load_ground_terminal_profiles_explicit(cursor: Any) -> dict[str, dict[str, Any]]:
    if not _relation_exists(cursor, f"{EXPLICIT_SCHEMA}.ground_terminal_profile"):
        return {}
    fields = (*_GROUND_EXPLICIT_FLOAT_FIELDS, *_GROUND_EXPLICIT_TEXT_FIELDS)
    rows = _fetch_rows(
        cursor,
        f"""
        SELECT profile_key, station_kind, {", ".join(fields)}
        FROM {EXPLICIT_SCHEMA}.ground_terminal_profile
        ORDER BY profile_key
        """,
        (),
    )
    profiles: dict[str, dict[str, Any]] = {}
    for row in rows:
        profiles[str(row["profile_key"])] = _profile_payload_from_explicit_row(
            row,
            mode_field="station_kind",
            fields=fields,
            storage_model=f"{EXPLICIT_SCHEMA}.ground_terminal_profile",
        )
    return profiles


def _load_relay_payload_profiles_v2(cursor: Any) -> dict[str, dict[str, Any]]:
    legacy_profiles = _load_relay_payload_profiles_legacy_v2(cursor)
    explicit_profiles = _load_relay_payload_profiles_explicit(cursor)
    return {**legacy_profiles, **explicit_profiles}


def _load_relay_payload_profiles_legacy_v2(cursor: Any) -> dict[str, dict[str, Any]]:
    rows = _fetch_rows(
        cursor,
        f"""
        SELECT profile_key, relay_mode, params, metadata
        FROM {INVENTORY_V2_SCHEMA}.relay_payload_profile
        ORDER BY profile_key
        """,
        (),
    )
    profiles: dict[str, dict[str, Any]] = {}
    for row in rows:
        params = dict(row["params"]) if isinstance(row.get("params"), dict) else {}
        metadata = dict(row["metadata"]) if isinstance(row.get("metadata"), dict) else {}
        profiles[str(row["profile_key"])] = {
            "relay_mode": str(row["relay_mode"]),
            **params,
            "metadata": metadata,
        }
    return profiles


def _load_relay_payload_profiles_explicit(cursor: Any) -> dict[str, dict[str, Any]]:
    if not _relation_exists(cursor, f"{EXPLICIT_SCHEMA}.relay_payload_profile"):
        return {}
    fields = (*_RELAY_EXPLICIT_FLOAT_FIELDS, *_RELAY_EXPLICIT_TEXT_FIELDS)
    rows = _fetch_rows(
        cursor,
        f"""
        SELECT profile_key, relay_mode, {", ".join(fields)}
        FROM {EXPLICIT_SCHEMA}.relay_payload_profile
        ORDER BY profile_key
        """,
        (),
    )
    profiles: dict[str, dict[str, Any]] = {}
    for row in rows:
        profiles[str(row["profile_key"])] = _profile_payload_from_explicit_row(
            row,
            mode_field="relay_mode",
            fields=fields,
            storage_model=f"{EXPLICIT_SCHEMA}.relay_payload_profile",
        )
    return profiles


def _profile_payload_from_explicit_row(
    row: dict[str, Any],
    *,
    mode_field: str,
    fields: tuple[str, ...],
    storage_model: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {mode_field: str(row[mode_field])}
    for field_name in fields:
        value = row.get(field_name)
        if value is not None:
            payload[field_name] = value
    payload["metadata"] = {"storage_model": storage_model}
    return payload


def _merge_explicit_inventory_rows(cursor: Any, *, satellites: list[dict[str, Any]], haps: list[dict[str, Any]]) -> None:
    _merge_explicit_resource_limits_into_rows(cursor, "satellites", satellites)
    _merge_explicit_resource_limits_into_rows(cursor, "haps", haps)
    _merge_explicit_allowed_link_types_into_rows(cursor, "satellites", satellites)
    _merge_explicit_allowed_link_types_into_rows(cursor, "haps", haps)


def _merge_explicit_resource_limits_into_rows(cursor: Any, kind: str, rows: list[dict[str, Any]]) -> None:
    config = _RESOURCE_LIMIT_EXPLICIT_CONFIG[kind]
    explicit = _load_explicit_resource_limits(cursor, kind, tuple(str(row["id"]) for row in rows))
    if not explicit:
        return
    for row in rows:
        key = str(row["id"])
        explicit_limits = explicit.get(key)
        if not explicit_limits:
            continue
        old_limits = row.get("resource_limits")
        merged = dict(old_limits) if isinstance(old_limits, dict) else {}
        merged.update(explicit_limits)
        row["resource_limits"] = merged
        metadata = dict(row.get("metadata", {})) if isinstance(row.get("metadata"), dict) else {}
        metadata["resource_limits_storage_model"] = f"{EXPLICIT_SCHEMA}.{config['table']}"
        row["metadata"] = metadata


def _load_explicit_resource_limits(
    cursor: Any,
    kind: str,
    entity_keys: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    config = _RESOURCE_LIMIT_EXPLICIT_CONFIG[kind]
    if not entity_keys or not _relation_exists(cursor, f"{EXPLICIT_SCHEMA}.{config['table']}"):
        return {}
    fields = tuple(config["fields"])
    rows = _fetch_rows(
        cursor,
        f"""
        SELECT {config["key_column"]}, {", ".join(fields)}
        FROM {EXPLICIT_SCHEMA}.{config["table"]}
        WHERE {config["key_column"]} = ANY(%s::text[])
        """,
        (list(entity_keys),),
    )
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        values = {field_name: row[field_name] for field_name in fields if row.get(field_name) is not None}
        if values:
            result[str(row[config["key_column"]])] = values
    return result


def _merge_explicit_allowed_link_types_into_rows(cursor: Any, kind: str, rows: list[dict[str, Any]]) -> None:
    config = _ALLOWED_LINK_EXPLICIT_CONFIG[kind]
    explicit = _load_explicit_allowed_link_types(cursor, kind, tuple(str(row["id"]) for row in rows))
    if not explicit:
        return
    for row in rows:
        link_types = explicit.get(str(row["id"]))
        if link_types:
            row["allowed_link_types"] = list(link_types)
            metadata = dict(row.get("metadata", {})) if isinstance(row.get("metadata"), dict) else {}
            metadata["allowed_link_types_storage_model"] = f"{EXPLICIT_SCHEMA}.{config['table']}"
            row["metadata"] = metadata


def _load_explicit_allowed_link_types(
    cursor: Any,
    kind: str,
    entity_keys: tuple[str, ...],
) -> dict[str, tuple[str, ...]]:
    config = _ALLOWED_LINK_EXPLICIT_CONFIG[kind]
    if not entity_keys or not _relation_exists(cursor, f"{EXPLICIT_SCHEMA}.{config['table']}"):
        return {}
    rows = _fetch_rows(
        cursor,
        f"""
        SELECT {config["key_column"]}, link_type
        FROM {EXPLICIT_SCHEMA}.{config["table"]}
        WHERE {config["key_column"]} = ANY(%s::text[])
        ORDER BY {config["key_column"]}, link_type
        """,
        (list(entity_keys),),
    )
    grouped: dict[str, list[str]] = {}
    for row in rows:
        grouped.setdefault(str(row[config["key_column"]]), []).append(str(row["link_type"]))
    return {key: tuple(values) for key, values in grouped.items()}


def _load_inventory_bundle_v1(cursor: Any, inventory_db: Any) -> InventoryBundle:
    schema = INVENTORY_SCHEMA
    gateways = _fetch_rows(
        cursor,
        f"""
        SELECT external_id AS id, name, lat, lon, site_alt_m, antenna_height_agl_m,
               COALESCE(radio_profile, 'default') AS radio_profile,
               COALESCE(role, 'gateway') AS role,
               COALESCE(limits, '{{}}'::jsonb) AS limits,
               COALESCE(metadata, '{{}}'::jsonb) AS metadata
        FROM {schema}.gateway_node
        WHERE set_key = %s
        ORDER BY ordinal, external_id
        """,
        (_selection_value(inventory_db, "gateways", "cgs_set"),),
    )
    targets = _fetch_rows(
        cursor,
        f"""
        SELECT external_id AS id, name, lat, lon, frequency, priority,
               site_alt_m, antenna_height_agl_m,
               COALESCE(metadata, '{{}}'::jsonb) AS metadata
        FROM {schema}.target_hub
        WHERE set_key = %s
        ORDER BY ordinal, external_id
        """,
        (_selection_value(inventory_db, "targets", "targets_set"),),
    )
    satellites = _fetch_rows(
        cursor,
        f"""
        SELECT external_id AS id, name, tle_line1, tle_line2,
               COALESCE(radio_profile, 'default') AS radio_profile,
               connection_min, beam_layout_mode, dynamic_ray_count,
               dynamic_ray_aperture_deg, sat_haps_ray_count,
               COALESCE(resource_limits, '{{}}'::jsonb) AS resource_limits,
               COALESCE(allowed_link_types, '[]'::jsonb) AS allowed_link_types,
               COALESCE(metadata, '{{}}'::jsonb) AS metadata
        FROM {schema}.satellite_node
        WHERE set_key = %s
        ORDER BY ordinal, external_id
        """,
        (_selection_value(inventory_db, "satellites", "sat_set"),),
    )
    haps = _fetch_rows(
        cursor,
        f"""
        SELECT external_id AS id, name, lat, lon, alt_m,
               COALESCE(radio_profile, 'default') AS radio_profile,
               connection_min,
               COALESCE(resource_limits, '{{}}'::jsonb) AS resource_limits,
               COALESCE(metadata, '{{}}'::jsonb) AS metadata
        FROM {schema}.haps_node
        WHERE set_key = %s
        ORDER BY ordinal, external_id
        """,
        (_selection_value(inventory_db, "haps", "haps_set"),),
    )
    vsats = _fetch_rows(
        cursor,
        f"""
        SELECT external_id AS id, name, lat, lon, region_code,
               site_alt_m, antenna_height_agl_m,
               COALESCE(radio_profile, 'default') AS radio_profile,
               COALESCE(role, 'vsat') AS role,
               COALESCE(limits, '{{}}'::jsonb) AS limits,
               COALESCE(metadata, '{{}}'::jsonb) AS metadata
        FROM {schema}.vsat_node
        WHERE set_key = %s
        ORDER BY ordinal, external_id
        """,
        (_selection_value(inventory_db, "vsats", "vsat_set"),),
    )
    return InventoryBundle(
        gateways=gateways,
        targets=targets,
        satellites=satellites,
        haps=haps,
        vsats=vsats,
        theoretical_subscribers=[],
        ground_terminal_profiles={},
        relay_payload_profiles={},
    )


def _load_inventory_bundle_v2(cursor: Any, inventory_db: Any) -> InventoryBundle:
    schema = INVENTORY_V2_SCHEMA
    gateways = _fetch_rows(
        cursor,
        f"""
        SELECT g.gateway_key AS id, g.name, g.lat, g.lon, g.site_alt_m, g.antenna_height_agl_m,
               COALESCE(g.radio_profile, 'default') AS radio_profile,
               COALESCE(g.ground_terminal_profile_key, g.radio_profile, 'default') AS ground_terminal_profile_key,
               COALESCE(g.role, 'gateway') AS role,
               COALESCE(g.limits, '{{}}'::jsonb) AS limits,
               COALESCE(g.metadata, '{{}}'::jsonb) AS metadata
        FROM {schema}.selection_catalog c
        JOIN {schema}.gateway_catalog_member m ON m.catalog_id = c.catalog_id
        JOIN {schema}.gateway g ON g.gateway_id = m.gateway_id
        WHERE c.kind = 'gateways' AND c.catalog_key = %s
        ORDER BY m.ordinal, g.gateway_key
        """,
        (_selection_value(inventory_db, "gateways", "cgs_set"),),
    )
    targets = _fetch_rows(
        cursor,
        f"""
        SELECT t.target_key AS id, t.name, t.lat, t.lon, t.frequency, t.priority,
               t.site_alt_m, t.antenna_height_agl_m,
               t.ground_terminal_profile_key,
               COALESCE(t.metadata, '{{}}'::jsonb) AS metadata
        FROM {schema}.selection_catalog c
        JOIN {schema}.target_catalog_member m ON m.catalog_id = c.catalog_id
        JOIN {schema}.target t ON t.target_id = m.target_id
        WHERE c.kind = 'targets' AND c.catalog_key = %s
        ORDER BY m.ordinal, t.target_key
        """,
        (_selection_value(inventory_db, "targets", "targets_set"),),
    )
    satellites = _fetch_rows(
        cursor,
        f"""
        SELECT s.satellite_key AS id, s.name, s.tle_line1, s.tle_line2,
               COALESCE(s.radio_profile, 'default') AS radio_profile,
               COALESCE(s.user_beam_profile_key, s.radio_profile, 'default') AS user_beam_profile_key,
               COALESCE(s.feeder_beam_profile_key, s.radio_profile, 'default') AS feeder_beam_profile_key,
               s.connection_min, s.beam_layout_mode, s.dynamic_ray_count,
               s.dynamic_ray_aperture_deg, s.sat_haps_ray_count,
               COALESCE(s.resource_limits, '{{}}'::jsonb) AS resource_limits,
               COALESCE(s.allowed_link_types, '[]'::jsonb) AS allowed_link_types,
               COALESCE(s.metadata, '{{}}'::jsonb) AS metadata
        FROM {schema}.selection_catalog c
        JOIN {schema}.satellite_catalog_member m ON m.catalog_id = c.catalog_id
        JOIN {schema}.satellite s ON s.satellite_id = m.satellite_id
        WHERE c.kind = 'satellites' AND c.catalog_key = %s
        ORDER BY m.ordinal, s.satellite_key
        """,
        (_selection_value(inventory_db, "satellites", "sat_set"),),
    )
    haps = _fetch_rows(
        cursor,
        f"""
        SELECT h.haps_key AS id, h.name, h.lat, h.lon, h.alt_m,
               COALESCE(h.radio_profile, 'default') AS radio_profile,
               COALESCE(h.user_beam_profile_key, h.radio_profile, 'default') AS user_beam_profile_key,
               COALESCE(h.feeder_beam_profile_key, h.radio_profile, 'default') AS feeder_beam_profile_key,
               h.connection_min,
               COALESCE(h.resource_limits, '{{}}'::jsonb) AS resource_limits,
               COALESCE(h.metadata, '{{}}'::jsonb) AS metadata
        FROM {schema}.selection_catalog c
        JOIN {schema}.haps_catalog_member m ON m.catalog_id = c.catalog_id
        JOIN {schema}.haps h ON h.haps_id = m.haps_id
        WHERE c.kind = 'haps' AND c.catalog_key = %s
        ORDER BY m.ordinal, h.haps_key
        """,
        (_selection_value(inventory_db, "haps", "haps_set"),),
    )
    vsats = _fetch_rows(
        cursor,
        f"""
        SELECT v.vsat_key AS id, v.name, v.lat, v.lon, v.region_code,
               v.site_alt_m, v.antenna_height_agl_m,
               COALESCE(v.radio_profile, 'default') AS radio_profile,
               COALESCE(v.ground_terminal_profile_key, v.radio_profile, 'default') AS ground_terminal_profile_key,
               COALESCE(v.role, 'vsat') AS role,
               COALESCE(v.limits, '{{}}'::jsonb) AS limits,
               COALESCE(v.metadata, '{{}}'::jsonb) AS metadata
        FROM {schema}.selection_catalog c
        JOIN {schema}.vsat_catalog_member m ON m.catalog_id = c.catalog_id
        JOIN {schema}.vsat v ON v.vsat_id = m.vsat_id
        WHERE c.kind = 'vsats' AND c.catalog_key = %s
        ORDER BY m.ordinal, v.vsat_key
        """,
        (_selection_value(inventory_db, "vsats", "vsat_set"),),
    )
    theoretical_subscribers = _fetch_rows(
        cursor,
        f"""
        SELECT t.subscriber_key AS id, t.name, t.lat, t.lon, t.site_alt_m,
               t.subject_code, t.subject_name, t.federal_district,
               t.grid_cell_id, t.seed_version, t.is_active,
               COALESCE(t.ground_terminal_profile_key, 'default') AS ground_terminal_profile_key,
               COALESCE(t.metadata, '{{}}'::jsonb) AS metadata
        FROM {schema}.selection_catalog c
        JOIN {schema}.theoretical_subscriber_catalog_member m ON m.catalog_id = c.catalog_id
        JOIN {schema}.theoretical_subscriber t ON t.theoretical_subscriber_id = m.theoretical_subscriber_id
        WHERE c.kind = 'theoretical_subscribers' AND c.catalog_key = %s
        ORDER BY m.ordinal, t.subscriber_key
        """,
        (_selection_value(inventory_db, "theoretical_subscribers", "theoretical_subscribers_set"),),
    )
    _merge_explicit_inventory_rows(cursor, satellites=satellites, haps=haps)
    return InventoryBundle(
        gateways=gateways,
        targets=targets,
        satellites=satellites,
        haps=haps,
        vsats=vsats,
        theoretical_subscribers=theoretical_subscribers,
        ground_terminal_profiles=_load_ground_terminal_profiles_v2(cursor),
        relay_payload_profiles=_load_relay_payload_profiles_v2(cursor),
    )


def load_inventory_bundle(
    inventory_db: Any,
    postgres: PostgresSettings | None = None,
) -> InventoryBundle:
    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()
    source = _selection_source(inventory_db)
    if source not in SUPPORTED_INVENTORY_SOURCES:
        raise ValueError(f"Unsupported inventory_db.source: {source}")

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            if source == "postgres_v2":
                return _load_inventory_bundle_v2(cursor, inventory_db)
            return _load_inventory_bundle_v1(cursor, inventory_db)
