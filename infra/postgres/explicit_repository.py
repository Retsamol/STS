from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from simulation_core.inventory_selection import REQUIRED_INVENTORY_KINDS

from .inventory_store import _connection_kwargs, _fetch_rows, _require_psycopg
from .settings import PostgresSettings, load_postgres_settings


EXPLICIT_SCHEMA = "inventory_explicit"

GROUND_TERMINAL_PROFILE_FLOAT_FIELDS = (
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
GROUND_TERMINAL_PROFILE_TEXT_FIELDS = ("antenna_pattern_reference", "source_name")
GROUND_TERMINAL_PROFILE_REQUIRED_FIELDS = (
    "profile_key",
    "station_kind",
    "tx_power_dbw",
    "tx_antenna_gain_dbi",
    "tx_center_frequency_ghz",
    "tx_bandwidth_mhz",
    "tx_polarization_deg",
    "tx_waveguide_loss_db",
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

RELAY_PAYLOAD_PROFILE_FLOAT_FIELDS = (
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
RELAY_PAYLOAD_PROFILE_TEXT_FIELDS = ("antenna_pattern_reference", "source_name")
RELAY_PAYLOAD_PROFILE_REQUIRED_FIELDS = (
    "profile_key",
    "relay_mode",
    "eirp_sat_dbw",
    "gt_dbk",
    "tx_power_dbw",
    "tx_center_frequency_ghz",
    "tx_bandwidth_mhz",
    "tx_antenna_gain_dbi",
    "tx_polarization_deg",
    "tx_waveguide_loss_db",
    "rx_center_frequency_ghz",
    "rx_bandwidth_mhz",
    "rx_antenna_gain_dbi",
    "rx_polarization_deg",
    "rx_waveguide_loss_db",
    "rx_noise_temperature_k",
    "off_axis_loss_db_per_rad",
)
_PROFILE_CONTROL_FIELDS = {"params", "metadata", "profile_key", "station_kind", "relay_mode"}

SUPPORTED_EXPLICIT_LINK_TYPES = {
    "sat_to_cgs",
    "haps_to_cgs",
    "sat_to_vsat",
    "haps_to_vsat",
    "sat_to_haps",
    "sat_to_sat",
    "haps_to_haps",
}
_RESOURCE_LIMIT_CONFIG = {
    "satellites": {
        "table": "satellite_resource_limit",
        "key_column": "satellite_key",
        "fields": ("max_user_links", "max_feeder_links", "max_interobject_links", "access_model"),
        "integer_fields": ("max_user_links", "max_feeder_links", "max_interobject_links"),
        "float_fields": (),
        "text_fields": ("access_model",),
        "required_fields": ("max_user_links", "max_feeder_links", "max_interobject_links"),
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
        "integer_fields": (
            "max_user_links",
            "max_feeder_links",
            "max_haps_links",
            "cgs_ray_count",
            "feeder_ray_count",
        ),
        "float_fields": ("angle_rad", "beam_angle_rad", "beam_angle_deg", "maxlength"),
        "text_fields": (),
        "required_fields": (),
    },
}
_ALLOWED_LINK_TYPE_CONFIG = {
    "satellites": {
        "table": "satellite_allowed_link_type",
        "key_column": "satellite_key",
    },
    "haps": {
        "table": "haps_allowed_link_type",
        "key_column": "haps_key",
    },
}
_ENTITY_CONFIG = {
    "gateways": {
        "table": "gateway",
        "key_column": "gateway_key",
        "key_aliases": ("gateway_key", "entity_key", "key", "id"),
        "required": ("name", "lat", "lon"),
        "optional": (
            "site_alt_m",
            "antenna_height_agl_m",
            "radio_profile",
            "ground_terminal_profile_key",
            "role",
            "connect_limit",
            "capacity_mbps",
            "bandwidth_mhz",
            "spectral_efficiency_bps_hz",
            "source_name",
        ),
        "defaults": {"role": "gateway"},
        "float_fields": (
            "lat",
            "lon",
            "site_alt_m",
            "antenna_height_agl_m",
            "capacity_mbps",
            "bandwidth_mhz",
            "spectral_efficiency_bps_hz",
        ),
        "int_fields": ("connect_limit",),
        "bool_fields": (),
        "text_fields": (
            "name",
            "radio_profile",
            "ground_terminal_profile_key",
            "role",
            "source_name",
        ),
    },
    "targets": {
        "table": "target",
        "key_column": "target_key",
        "key_aliases": ("target_key", "entity_key", "key", "id"),
        "required": ("name", "lat", "lon", "frequency"),
        "optional": (
            "priority",
            "site_alt_m",
            "antenna_height_agl_m",
            "ground_terminal_profile_key",
            "source_name",
        ),
        "defaults": {"priority": 0.0},
        "float_fields": ("lat", "lon", "priority", "site_alt_m", "antenna_height_agl_m"),
        "int_fields": ("frequency",),
        "bool_fields": (),
        "text_fields": ("name", "ground_terminal_profile_key", "source_name"),
    },
    "satellites": {
        "table": "satellite",
        "key_column": "satellite_key",
        "key_aliases": ("satellite_key", "entity_key", "key", "id"),
        "required": ("name", "tle_line1", "tle_line2"),
        "optional": (
            "radio_profile",
            "user_beam_profile_key",
            "feeder_beam_profile_key",
            "connection_min",
            "beam_layout_mode",
            "dynamic_ray_count",
            "dynamic_ray_aperture_deg",
            "sat_haps_ray_count",
            "source_name",
        ),
        "defaults": {
            "connection_min": 2,
            "beam_layout_mode": "free",
            "dynamic_ray_count": 16,
            "dynamic_ray_aperture_deg": 1.5,
            "sat_haps_ray_count": 4,
        },
        "float_fields": ("dynamic_ray_aperture_deg",),
        "int_fields": ("connection_min", "dynamic_ray_count", "sat_haps_ray_count"),
        "bool_fields": (),
        "text_fields": (
            "name",
            "tle_line1",
            "tle_line2",
            "radio_profile",
            "user_beam_profile_key",
            "feeder_beam_profile_key",
            "beam_layout_mode",
            "source_name",
        ),
    },
    "haps": {
        "table": "haps",
        "key_column": "haps_key",
        "key_aliases": ("haps_key", "entity_key", "key", "id"),
        "required": ("name", "lat", "lon", "alt_m"),
        "optional": (
            "radio_profile",
            "user_beam_profile_key",
            "feeder_beam_profile_key",
            "connection_min",
            "source_name",
        ),
        "defaults": {"connection_min": 0},
        "float_fields": ("lat", "lon", "alt_m"),
        "int_fields": ("connection_min",),
        "bool_fields": (),
        "text_fields": (
            "name",
            "radio_profile",
            "user_beam_profile_key",
            "feeder_beam_profile_key",
            "source_name",
        ),
    },
    "vsats": {
        "table": "vsat",
        "key_column": "vsat_key",
        "key_aliases": ("vsat_key", "entity_key", "key", "id"),
        "required": ("name", "lat", "lon", "region_code"),
        "optional": (
            "site_alt_m",
            "antenna_height_agl_m",
            "radio_profile",
            "ground_terminal_profile_key",
            "role",
            "connect_limit",
            "source_name",
        ),
        "defaults": {"role": "vsat"},
        "float_fields": ("lat", "lon", "site_alt_m", "antenna_height_agl_m"),
        "int_fields": ("region_code", "connect_limit"),
        "bool_fields": (),
        "text_fields": (
            "name",
            "radio_profile",
            "ground_terminal_profile_key",
            "role",
            "source_name",
        ),
    },
    "theoretical_subscribers": {
        "table": "theoretical_subscriber",
        "key_column": "subscriber_key",
        "key_aliases": ("subscriber_key", "theoretical_subscriber_key", "entity_key", "key", "id"),
        "required": (
            "name",
            "lat",
            "lon",
            "subject_code",
            "subject_name",
            "federal_district",
            "grid_cell_id",
            "seed_version",
        ),
        "optional": (
            "site_alt_m",
            "ground_terminal_profile_key",
            "is_active",
            "source_name",
        ),
        "defaults": {"is_active": True},
        "float_fields": ("lat", "lon", "site_alt_m"),
        "int_fields": ("subject_code",),
        "bool_fields": ("is_active",),
        "text_fields": (
            "name",
            "subject_name",
            "federal_district",
            "grid_cell_id",
            "seed_version",
            "ground_terminal_profile_key",
            "source_name",
        ),
    },
}
_ENTITY_CONTROL_FIELDS = {
    "kind",
    "entity_key",
    "key",
    "id",
    "metadata",
    "catalog_key",
    "catalog_name",
    "catalog_description",
    "catalog_ordinal",
    "source_type",
}


@dataclass(slots=True)
class ExplicitScenarioTimeline:
    start_time_utc: datetime
    end_time_utc: datetime
    time_step_sec: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_time_utc": _time_to_iso(self.start_time_utc),
            "end_time_utc": _time_to_iso(self.end_time_utc),
            "time_step_sec": int(self.time_step_sec),
        }


@dataclass(slots=True)
class ExplicitGroundTerminalProfileRecord:
    profile_key: str
    station_kind: str
    values: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_key": str(self.profile_key),
            "station_kind": str(self.station_kind),
            **dict(self.values),
            "metadata": dict(self.metadata),
        }

    def to_legacy_params(self) -> dict[str, Any]:
        return {
            "station_kind": str(self.station_kind),
            **dict(self.values),
        }


@dataclass(slots=True)
class ExplicitRelayPayloadProfileRecord:
    profile_key: str
    relay_mode: str
    values: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_key": str(self.profile_key),
            "relay_mode": str(self.relay_mode),
            **dict(self.values),
            "metadata": dict(self.metadata),
        }

    def to_legacy_params(self) -> dict[str, Any]:
        return {
            "relay_mode": str(self.relay_mode),
            **dict(self.values),
        }


@dataclass(slots=True)
class ExplicitResourceLimitRecord:
    kind: str
    entity_key: str
    values: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": str(self.kind),
            "entity_key": str(self.entity_key),
            **dict(self.values),
        }


@dataclass(slots=True)
class ExplicitAllowedLinkTypesRecord:
    kind: str
    entity_key: str
    link_types: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": str(self.kind),
            "entity_key": str(self.entity_key),
            "link_types": [str(item) for item in self.link_types],
        }


@dataclass(slots=True)
class ExplicitInventoryEntityRecord:
    kind: str
    entity_key: str
    values: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": str(self.kind),
            "entity_key": str(self.entity_key),
            **dict(self.values),
        }

    def to_legacy_payload(self) -> dict[str, Any]:
        payload = {
            key: value
            for key, value in self.values.items()
            if key not in {
                "connect_limit",
                "capacity_mbps",
                "bandwidth_mhz",
                "spectral_efficiency_bps_hz",
                "source_name",
            }
        }
        payload["entity_key"] = str(self.entity_key)
        return payload


@dataclass(slots=True)
class ExplicitScenarioNetworkSettings:
    selection_mode: str
    earth_model: str
    connectivity_mode: str
    min_elevation_deg: float | None = None
    target_elevation_deg: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "selection_mode": str(self.selection_mode),
            "earth_model": str(self.earth_model),
            "connectivity_mode": str(self.connectivity_mode),
            "min_elevation_deg": None if self.min_elevation_deg is None else float(self.min_elevation_deg),
            "target_elevation_deg": None if self.target_elevation_deg is None else float(self.target_elevation_deg),
        }


@dataclass(slots=True)
class ExplicitScenarioEntity:
    kind: str
    entity_key: str
    role: str
    enabled: bool = True
    ordinal: int = 0
    ground_terminal_profile_key: str | None = None
    user_beam_profile_key: str | None = None
    feeder_beam_profile_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": str(self.kind),
            "entity_key": str(self.entity_key),
            "role": str(self.role),
            "enabled": bool(self.enabled),
            "ordinal": int(self.ordinal),
            "ground_terminal_profile_key": self.ground_terminal_profile_key,
            "user_beam_profile_key": self.user_beam_profile_key,
            "feeder_beam_profile_key": self.feeder_beam_profile_key,
        }


@dataclass(slots=True)
class ExplicitScenarioTrafficFlow:
    flow_key: str
    source_kind: str
    source_key: str
    target_kind: str
    target_key: str
    requested_rate_mbps: float
    priority: int = 100
    start_time_utc: datetime | None = None
    end_time_utc: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "flow_key": str(self.flow_key),
            "source_kind": str(self.source_kind),
            "source_key": str(self.source_key),
            "target_kind": str(self.target_kind),
            "target_key": str(self.target_key),
            "requested_rate_mbps": float(self.requested_rate_mbps),
            "priority": int(self.priority),
            "start_time_utc": None if self.start_time_utc is None else _time_to_iso(self.start_time_utc),
            "end_time_utc": None if self.end_time_utc is None else _time_to_iso(self.end_time_utc),
        }


@dataclass(slots=True)
class ExplicitScenarioSummary:
    scenario_id: int
    scenario_key: str
    name: str
    description: str | None
    status: str
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": int(self.scenario_id),
            "scenario_key": str(self.scenario_key),
            "name": str(self.name),
            "description": self.description,
            "status": str(self.status),
            "created_at": None if self.created_at is None else _time_to_iso(self.created_at),
            "updated_at": None if self.updated_at is None else _time_to_iso(self.updated_at),
        }


@dataclass(slots=True)
class ExplicitScenarioDraft:
    scenario_key: str
    name: str
    timeline: ExplicitScenarioTimeline
    network_settings: ExplicitScenarioNetworkSettings
    entities: tuple[ExplicitScenarioEntity, ...]
    traffic_flows: tuple[ExplicitScenarioTrafficFlow, ...] = field(default_factory=tuple)
    scenario_id: int | None = None
    description: str | None = None
    status: str = "draft"
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": None if self.scenario_id is None else int(self.scenario_id),
            "scenario_key": str(self.scenario_key),
            "name": str(self.name),
            "description": self.description,
            "status": str(self.status),
            "created_at": None if self.created_at is None else _time_to_iso(self.created_at),
            "updated_at": None if self.updated_at is None else _time_to_iso(self.updated_at),
            "timeline": self.timeline.to_dict(),
            "network_settings": self.network_settings.to_dict(),
            "entities": [item.to_dict() for item in self.entities],
            "traffic_flows": [item.to_dict() for item in self.traffic_flows],
        }


def ensure_explicit_schema(postgres: PostgresSettings | None = None) -> None:
    postgres = postgres or load_postgres_settings()
    from .migrations import apply_migrations

    apply_migrations(postgres, target_version="0004_explicit_resource_limit_runtime_fields")


def build_explicit_ground_terminal_profile(
    payload: Mapping[str, Any],
    *,
    profile_key: str | None = None,
) -> ExplicitGroundTerminalProfileRecord:
    data = _profile_payload(payload)
    key = _optional_str(profile_key) or _required_str(data, "profile_key")
    station_kind = _required_str(data, "station_kind")
    values = _profile_values(
        data,
        float_fields=GROUND_TERMINAL_PROFILE_FLOAT_FIELDS,
        text_fields=GROUND_TERMINAL_PROFILE_TEXT_FIELDS,
        required_fields=GROUND_TERMINAL_PROFILE_REQUIRED_FIELDS,
        key_fields={"profile_key": key, "station_kind": station_kind},
    )
    return ExplicitGroundTerminalProfileRecord(
        profile_key=key,
        station_kind=station_kind,
        values=values,
        metadata=_profile_metadata(payload),
    )


def build_explicit_relay_payload_profile(
    payload: Mapping[str, Any],
    *,
    profile_key: str | None = None,
) -> ExplicitRelayPayloadProfileRecord:
    data = _profile_payload(payload)
    _derive_relay_tx_power_dbw(data)
    key = _optional_str(profile_key) or _required_str(data, "profile_key")
    relay_mode = _optional_str(data.get("relay_mode")) or "transparent_relay"
    values = _profile_values(
        data,
        float_fields=RELAY_PAYLOAD_PROFILE_FLOAT_FIELDS,
        text_fields=RELAY_PAYLOAD_PROFILE_TEXT_FIELDS,
        required_fields=RELAY_PAYLOAD_PROFILE_REQUIRED_FIELDS,
        key_fields={"profile_key": key, "relay_mode": relay_mode},
    )
    return ExplicitRelayPayloadProfileRecord(
        profile_key=key,
        relay_mode=relay_mode,
        values=values,
        metadata=_profile_metadata(payload),
    )


def save_explicit_ground_terminal_profile(
    record: ExplicitGroundTerminalProfileRecord,
    *,
    postgres: PostgresSettings | None = None,
) -> ExplicitGroundTerminalProfileRecord:
    postgres = postgres or load_postgres_settings()
    ensure_explicit_schema(postgres)
    psycopg = _require_psycopg()
    columns = ("profile_key", "station_kind", *GROUND_TERMINAL_PROFILE_FLOAT_FIELDS, *GROUND_TERMINAL_PROFILE_TEXT_FIELDS)
    values = [record.profile_key, record.station_kind, *(record.values.get(column) for column in columns[2:])]
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {EXPLICIT_SCHEMA}.ground_terminal_profile (
                    {", ".join(columns)}
                )
                VALUES ({", ".join(["%s"] * len(columns))})
                ON CONFLICT (profile_key) DO UPDATE SET
                    {", ".join(f"{column} = EXCLUDED.{column}" for column in columns[1:])},
                    updated_at = now()
                """,
                tuple(values),
            )
        connection.commit()
    return record


def save_explicit_relay_payload_profile(
    record: ExplicitRelayPayloadProfileRecord,
    *,
    postgres: PostgresSettings | None = None,
) -> ExplicitRelayPayloadProfileRecord:
    postgres = postgres or load_postgres_settings()
    ensure_explicit_schema(postgres)
    psycopg = _require_psycopg()
    columns = ("profile_key", "relay_mode", *RELAY_PAYLOAD_PROFILE_FLOAT_FIELDS, *RELAY_PAYLOAD_PROFILE_TEXT_FIELDS)
    values = [record.profile_key, record.relay_mode, *(record.values.get(column) for column in columns[2:])]
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {EXPLICIT_SCHEMA}.relay_payload_profile (
                    {", ".join(columns)}
                )
                VALUES ({", ".join(["%s"] * len(columns))})
                ON CONFLICT (profile_key) DO UPDATE SET
                    {", ".join(f"{column} = EXCLUDED.{column}" for column in columns[1:])},
                    updated_at = now()
                """,
                tuple(values),
            )
        connection.commit()
    return record


def build_explicit_resource_limits(
    kind: str,
    entity_key: str,
    payload: Mapping[str, Any],
) -> ExplicitResourceLimitRecord:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _RESOURCE_LIMIT_CONFIG:
        raise ValueError("resource limits are supported only for satellites and haps")
    normalized_key = _required_text(entity_key, "entity_key")
    if not isinstance(payload, Mapping):
        raise ValueError("resource limits payload must be an object")
    config = _RESOURCE_LIMIT_CONFIG[normalized_kind]
    allowed = set(config["fields"])
    unknown = sorted(str(key) for key in payload if str(key) not in allowed)
    if unknown:
        raise ValueError(
            "unsupported resource limit fields: "
            + ", ".join(unknown)
            + "; add explicit DB columns before accepting them"
        )
    missing = [
        field_name
        for field_name in config["required_fields"]
        if payload.get(field_name) is None
    ]
    if missing:
        raise ValueError("resource limits are missing required fields: " + ", ".join(missing))
    values: dict[str, Any] = {}
    for field_name in config["integer_fields"]:
        value = payload.get(field_name)
        if value is None:
            continue
        normalized_value = int(value)
        if normalized_value < 0:
            raise ValueError(f"{field_name} must be >= 0")
        values[field_name] = normalized_value
    for field_name in config["float_fields"]:
        value = payload.get(field_name)
        if value is None:
            continue
        normalized_value = float(value)
        if normalized_value < 0:
            raise ValueError(f"{field_name} must be >= 0")
        values[field_name] = normalized_value
    for field_name in config.get("text_fields", ()):
        value = payload.get(field_name)
        if value is None:
            continue
        normalized_value = str(value).strip()
        if not normalized_value:
            continue
        values[field_name] = normalized_value
    if not values:
        raise ValueError("resource limits payload must contain at least one explicit field")
    return ExplicitResourceLimitRecord(
        kind=normalized_kind,
        entity_key=normalized_key,
        values=values,
    )


def save_explicit_resource_limits(
    record: ExplicitResourceLimitRecord,
    *,
    postgres: PostgresSettings | None = None,
) -> ExplicitResourceLimitRecord:
    postgres = postgres or load_postgres_settings()
    ensure_explicit_schema(postgres)
    psycopg = _require_psycopg()
    config = _RESOURCE_LIMIT_CONFIG[record.kind]
    field_names = tuple(record.values)
    columns = (config["key_column"], *field_names)
    values = (record.entity_key, *(record.values[field_name] for field_name in field_names))
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {EXPLICIT_SCHEMA}.{config["table"]} (
                    {", ".join(columns)}
                )
                VALUES ({", ".join(["%s"] * len(columns))})
                ON CONFLICT ({config["key_column"]}) DO UPDATE SET
                    {", ".join(f"{field_name} = EXCLUDED.{field_name}" for field_name in field_names)}
                """,
                values,
            )
        connection.commit()
    return record


def load_explicit_resource_limits(
    kind: str,
    entity_key: str,
    *,
    postgres: PostgresSettings | None = None,
) -> ExplicitResourceLimitRecord | None:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _RESOURCE_LIMIT_CONFIG:
        raise ValueError("resource limits are supported only for satellites and haps")
    normalized_key = _required_text(entity_key, "entity_key")
    postgres = postgres or load_postgres_settings()
    ensure_explicit_schema(postgres)
    psycopg = _require_psycopg()
    config = _RESOURCE_LIMIT_CONFIG[normalized_kind]
    fields = tuple(config["fields"])
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT {", ".join(fields)}
                FROM {EXPLICIT_SCHEMA}.{config["table"]}
                WHERE {config["key_column"]} = %s
                """,
                (normalized_key,),
            )
    if not rows:
        return None
    values = {
        field_name: rows[0].get(field_name)
        for field_name in fields
        if rows[0].get(field_name) is not None
    }
    return ExplicitResourceLimitRecord(
        kind=normalized_kind,
        entity_key=normalized_key,
        values=values,
    )


def build_explicit_allowed_link_types(
    kind: str,
    entity_key: str,
    payload: Mapping[str, Any],
) -> ExplicitAllowedLinkTypesRecord:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _ALLOWED_LINK_TYPE_CONFIG:
        raise ValueError("allowed link types are supported only for satellites and haps")
    normalized_key = _required_text(entity_key, "entity_key")
    if not isinstance(payload, Mapping):
        raise ValueError("allowed link types payload must be an object")
    raw_link_types = payload.get("link_types", payload.get("allowed_link_types"))
    if not isinstance(raw_link_types, list):
        raise ValueError("link_types must be an array")
    link_types: list[str] = []
    for index, raw_value in enumerate(raw_link_types):
        link_type = _required_text(raw_value, f"link_types[{index}]")
        if link_type not in SUPPORTED_EXPLICIT_LINK_TYPES:
            raise ValueError(f"unsupported link type: {link_type}")
        if link_type not in link_types:
            link_types.append(link_type)
    if not link_types:
        raise ValueError("link_types must not be empty")
    return ExplicitAllowedLinkTypesRecord(
        kind=normalized_kind,
        entity_key=normalized_key,
        link_types=tuple(link_types),
    )


def save_explicit_allowed_link_types(
    record: ExplicitAllowedLinkTypesRecord,
    *,
    postgres: PostgresSettings | None = None,
) -> ExplicitAllowedLinkTypesRecord:
    postgres = postgres or load_postgres_settings()
    ensure_explicit_schema(postgres)
    psycopg = _require_psycopg()
    config = _ALLOWED_LINK_TYPE_CONFIG[record.kind]
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {EXPLICIT_SCHEMA}.{config['table']} WHERE {config['key_column']} = %s",
                (record.entity_key,),
            )
            for link_type in record.link_types:
                cursor.execute(
                    f"""
                    INSERT INTO {EXPLICIT_SCHEMA}.{config["table"]} (
                        {config["key_column"]}, link_type
                    )
                    VALUES (%s, %s)
                    """,
                    (record.entity_key, link_type),
                )
        connection.commit()
    return record


def load_explicit_allowed_link_types(
    kind: str,
    entity_key: str,
    *,
    postgres: PostgresSettings | None = None,
) -> ExplicitAllowedLinkTypesRecord | None:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _ALLOWED_LINK_TYPE_CONFIG:
        raise ValueError("allowed link types are supported only for satellites and haps")
    normalized_key = _required_text(entity_key, "entity_key")
    postgres = postgres or load_postgres_settings()
    ensure_explicit_schema(postgres)
    psycopg = _require_psycopg()
    config = _ALLOWED_LINK_TYPE_CONFIG[normalized_kind]
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT link_type
                FROM {EXPLICIT_SCHEMA}.{config["table"]}
                WHERE {config["key_column"]} = %s
                ORDER BY link_type
                """,
                (normalized_key,),
            )
    if not rows:
        return None
    return ExplicitAllowedLinkTypesRecord(
        kind=normalized_kind,
        entity_key=normalized_key,
        link_types=tuple(str(row["link_type"]) for row in rows),
    )


def build_explicit_inventory_entity(
    kind: str,
    payload: Mapping[str, Any],
    *,
    entity_key: str | None = None,
) -> ExplicitInventoryEntityRecord:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _ENTITY_CONFIG:
        raise ValueError(
            "explicit entity API supports gateways, targets, satellites, haps, vsats "
            "and theoretical_subscribers"
        )
    if not isinstance(payload, Mapping):
        raise ValueError("entity payload must be an object")

    config = _ENTITY_CONFIG[normalized_kind]
    key = _optional_str(entity_key)
    if key is None:
        for alias in config["key_aliases"]:
            key = _optional_str(payload.get(alias))
            if key is not None:
                break
    if key is None:
        raise ValueError(f"{config['key_column']} or entity_key is required")

    allowed_fields = (
        set(_ENTITY_CONTROL_FIELDS)
        | set(config["key_aliases"])
        | set(config["required"])
        | set(config["optional"])
    )
    unknown_fields = sorted(str(field) for field in payload if str(field) not in allowed_fields)
    if unknown_fields:
        raise ValueError(
            "unsupported entity fields: "
            + ", ".join(unknown_fields)
            + "; add explicit DB columns before accepting them"
        )

    values: dict[str, Any] = dict(config["defaults"])
    for field_name in (*config["required"], *config["optional"]):
        if field_name in payload:
            values[field_name] = payload[field_name]

    missing = [
        field_name
        for field_name in config["required"]
        if field_name not in values or _explicit_entity_field_missing(values.get(field_name))
    ]
    if missing:
        raise ValueError("entity is missing required calculation fields: " + ", ".join(missing))

    for field_name in config["float_fields"]:
        if field_name in values and values[field_name] is not None:
            values[field_name] = float(values[field_name])
    for field_name in config["int_fields"]:
        if field_name in values and values[field_name] is not None:
            values[field_name] = int(values[field_name])
    for field_name in config["bool_fields"]:
        if field_name in values and values[field_name] is not None:
            values[field_name] = _entity_bool(values[field_name])
    for field_name in config["text_fields"]:
        if field_name in values:
            values[field_name] = _optional_str(values[field_name])

    if not values.get("name"):
        raise ValueError("name is required")
    return ExplicitInventoryEntityRecord(
        kind=normalized_kind,
        entity_key=key,
        values=values,
    )


def save_explicit_inventory_entity(
    record: ExplicitInventoryEntityRecord,
    *,
    postgres: PostgresSettings | None = None,
) -> ExplicitInventoryEntityRecord:
    if record.kind not in _ENTITY_CONFIG:
        raise ValueError(f"unsupported explicit entity kind: {record.kind}")
    postgres = postgres or load_postgres_settings()
    ensure_explicit_schema(postgres)
    psycopg = _require_psycopg()
    config = _ENTITY_CONFIG[record.kind]
    field_names = (*config["required"], *config["optional"])
    columns = (config["key_column"], *field_names)
    values = (record.entity_key, *(record.values.get(field_name) for field_name in field_names))
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {EXPLICIT_SCHEMA}.{config["table"]} (
                    {", ".join(columns)}
                )
                VALUES ({", ".join(["%s"] * len(columns))})
                ON CONFLICT ({config["key_column"]}) DO UPDATE SET
                    {", ".join(f"{field_name} = EXCLUDED.{field_name}" for field_name in field_names)},
                    updated_at = now()
                """,
                values,
            )
        connection.commit()
    return record


def build_scenario_draft_from_payload(
    payload: Mapping[str, Any],
    *,
    scenario_key: str | None = None,
) -> ExplicitScenarioDraft:
    data = _scenario_payload(payload)
    name = _required_str(data, "name")
    key = _optional_str(scenario_key) or _optional_str(data.get("scenario_key")) or _new_scenario_key(name)
    timeline = _timeline_from_payload(data)
    network_settings = _network_settings_from_payload(data)
    entities = _entities_from_payload(data)
    traffic_flows = _traffic_flows_from_payload(data)
    if not entities:
        raise ValueError("scenario must contain at least one network entity")
    return ExplicitScenarioDraft(
        scenario_key=key,
        name=name,
        description=_optional_str(data.get("description")),
        status=str(data.get("status") or "draft"),
        timeline=timeline,
        network_settings=network_settings,
        entities=entities,
        traffic_flows=traffic_flows,
    )


def scenario_draft_to_run_request(draft: ExplicitScenarioDraft) -> dict[str, Any]:
    return {
        "name": draft.name,
        "selection": {
            "source": "inventory_explicit",
            "items": [
                {
                    "kind": entity.kind,
                    "entity_key": entity.entity_key,
                    "enabled": entity.enabled,
                    "ordinal": entity.ordinal,
                    **(
                        {}
                        if not _entity_profile_override_metadata(entity)
                        else {"metadata": _entity_profile_override_metadata(entity)}
                    ),
                }
                for entity in draft.entities
            ],
        },
        "simulation": {
            "start_time": _time_to_iso(draft.timeline.start_time_utc),
            "end_time": _time_to_iso(draft.timeline.end_time_utc),
            "time_step_sec": int(draft.timeline.time_step_sec),
            "selection_mode": draft.network_settings.selection_mode,
            "earth_model": draft.network_settings.earth_model,
        },
        "network": {
            "connectivity_mode": draft.network_settings.connectivity_mode,
            **(
                {}
                if draft.network_settings.min_elevation_deg is None
                else {"min_elevation_deg": float(draft.network_settings.min_elevation_deg)}
            ),
            **(
                {}
                if draft.network_settings.target_elevation_deg is None
                else {"target_elevation_deg": float(draft.network_settings.target_elevation_deg)}
            ),
        },
        "traffic": None
        if not draft.traffic_flows
        else {
            "flows": [
                {
                    "flow_id": flow.flow_key,
                    "src_external_id": flow.source_key,
                    "dst_external_id": flow.target_key,
                    "requested_rate_mbps": float(flow.requested_rate_mbps),
                    "priority": int(flow.priority),
                }
                for flow in draft.traffic_flows
            ]
        },
    }


def _entity_profile_override_metadata(entity: ExplicitScenarioEntity) -> dict[str, str]:
    metadata: dict[str, str] = {}
    if entity.ground_terminal_profile_key:
        metadata["ground_terminal_profile_key"] = str(entity.ground_terminal_profile_key)
    if entity.user_beam_profile_key:
        metadata["user_beam_profile_key"] = str(entity.user_beam_profile_key)
    if entity.feeder_beam_profile_key:
        metadata["feeder_beam_profile_key"] = str(entity.feeder_beam_profile_key)
    return metadata


def save_scenario_draft(
    draft: ExplicitScenarioDraft,
    *,
    postgres: PostgresSettings | None = None,
) -> ExplicitScenarioDraft:
    postgres = postgres or load_postgres_settings()
    ensure_explicit_schema(postgres)
    psycopg = _require_psycopg()
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                INSERT INTO {EXPLICIT_SCHEMA}.scenario (
                    scenario_key, name, description, status, updated_at
                )
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (scenario_key) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    status = EXCLUDED.status,
                    updated_at = now()
                RETURNING scenario_id, created_at, updated_at
                """,
                (draft.scenario_key, draft.name, draft.description, draft.status),
            )
            scenario_id = int(rows[0]["scenario_id"])
            cursor.execute(
                f"""
                INSERT INTO {EXPLICIT_SCHEMA}.scenario_timeline (
                    scenario_id, start_time_utc, end_time_utc, time_step_sec
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (scenario_id) DO UPDATE SET
                    start_time_utc = EXCLUDED.start_time_utc,
                    end_time_utc = EXCLUDED.end_time_utc,
                    time_step_sec = EXCLUDED.time_step_sec
                """,
                (
                    scenario_id,
                    draft.timeline.start_time_utc,
                    draft.timeline.end_time_utc,
                    draft.timeline.time_step_sec,
                ),
            )
            cursor.execute(
                f"""
                INSERT INTO {EXPLICIT_SCHEMA}.scenario_network_settings (
                    scenario_id, selection_mode, earth_model, min_elevation_deg,
                    target_elevation_deg, connectivity_mode
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (scenario_id) DO UPDATE SET
                    selection_mode = EXCLUDED.selection_mode,
                    earth_model = EXCLUDED.earth_model,
                    min_elevation_deg = EXCLUDED.min_elevation_deg,
                    target_elevation_deg = EXCLUDED.target_elevation_deg,
                    connectivity_mode = EXCLUDED.connectivity_mode
                """,
                (
                    scenario_id,
                    draft.network_settings.selection_mode,
                    draft.network_settings.earth_model,
                    draft.network_settings.min_elevation_deg,
                    draft.network_settings.target_elevation_deg,
                    draft.network_settings.connectivity_mode,
                ),
            )
            cursor.execute(
                f"DELETE FROM {EXPLICIT_SCHEMA}.scenario_entity WHERE scenario_id = %s",
                (scenario_id,),
            )
            cursor.execute(
                f"DELETE FROM {EXPLICIT_SCHEMA}.scenario_traffic_flow WHERE scenario_id = %s",
                (scenario_id,),
            )
            for entity in draft.entities:
                cursor.execute(
                    f"""
                    INSERT INTO {EXPLICIT_SCHEMA}.scenario_entity (
                        scenario_id, kind, entity_key, role, enabled, ordinal,
                        ground_terminal_profile_key, user_beam_profile_key, feeder_beam_profile_key
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        scenario_id,
                        entity.kind,
                        entity.entity_key,
                        entity.role,
                        entity.enabled,
                        entity.ordinal,
                        entity.ground_terminal_profile_key,
                        entity.user_beam_profile_key,
                        entity.feeder_beam_profile_key,
                    ),
                )
            for flow in draft.traffic_flows:
                cursor.execute(
                    f"""
                    INSERT INTO {EXPLICIT_SCHEMA}.scenario_traffic_flow (
                        scenario_id, flow_key, source_kind, source_key, target_kind, target_key,
                        requested_rate_mbps, priority, start_time_utc, end_time_utc
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        scenario_id,
                        flow.flow_key,
                        flow.source_kind,
                        flow.source_key,
                        flow.target_kind,
                        flow.target_key,
                        flow.requested_rate_mbps,
                        flow.priority,
                        flow.start_time_utc,
                        flow.end_time_utc,
                    ),
                )
        connection.commit()
    loaded = load_scenario_draft(draft.scenario_key, postgres=postgres)
    if loaded is None:
        raise RuntimeError("saved scenario draft could not be loaded")
    return loaded


def list_scenario_drafts(
    *,
    status: str | None = None,
    postgres: PostgresSettings | None = None,
) -> tuple[ExplicitScenarioSummary, ...]:
    postgres = postgres or load_postgres_settings()
    ensure_explicit_schema(postgres)
    where = ""
    params: tuple[Any, ...] = ()
    if status is not None:
        where = "WHERE status = %s"
        params = (status,)
    psycopg = _require_psycopg()
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT scenario_id, scenario_key, name, description, status, created_at, updated_at
                FROM {EXPLICIT_SCHEMA}.scenario
                {where}
                ORDER BY updated_at DESC, scenario_id DESC
                """,
                params,
            )
    return tuple(_summary_from_row(row) for row in rows)


def load_scenario_draft(
    identifier: str | int,
    *,
    postgres: PostgresSettings | None = None,
) -> ExplicitScenarioDraft | None:
    postgres = postgres or load_postgres_settings()
    ensure_explicit_schema(postgres)
    scenario_where, params = _scenario_identifier_condition(identifier)
    psycopg = _require_psycopg()
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            scenario_rows = _fetch_rows(
                cursor,
                f"""
                SELECT scenario_id, scenario_key, name, description, status, created_at, updated_at
                FROM {EXPLICIT_SCHEMA}.scenario
                WHERE {scenario_where}
                """,
                params,
            )
            if not scenario_rows:
                return None
            row = scenario_rows[0]
            scenario_id = int(row["scenario_id"])
            timeline_rows = _fetch_rows(
                cursor,
                f"""
                SELECT start_time_utc, end_time_utc, time_step_sec
                FROM {EXPLICIT_SCHEMA}.scenario_timeline
                WHERE scenario_id = %s
                """,
                (scenario_id,),
            )
            network_rows = _fetch_rows(
                cursor,
                f"""
                SELECT selection_mode, earth_model, min_elevation_deg, target_elevation_deg, connectivity_mode
                FROM {EXPLICIT_SCHEMA}.scenario_network_settings
                WHERE scenario_id = %s
                """,
                (scenario_id,),
            )
            entity_rows = _fetch_rows(
                cursor,
                f"""
                SELECT kind, entity_key, role, enabled, ordinal,
                       ground_terminal_profile_key, user_beam_profile_key, feeder_beam_profile_key
                FROM {EXPLICIT_SCHEMA}.scenario_entity
                WHERE scenario_id = %s
                ORDER BY kind, ordinal, entity_key
                """,
                (scenario_id,),
            )
            traffic_rows = _fetch_rows(
                cursor,
                f"""
                SELECT flow_key, source_kind, source_key, target_kind, target_key,
                       requested_rate_mbps, priority, start_time_utc, end_time_utc
                FROM {EXPLICIT_SCHEMA}.scenario_traffic_flow
                WHERE scenario_id = %s
                ORDER BY flow_key
                """,
                (scenario_id,),
            )
    if not timeline_rows:
        raise ValueError(f"scenario {identifier} has no timeline")
    if not network_rows:
        raise ValueError(f"scenario {identifier} has no network settings")
    return ExplicitScenarioDraft(
        scenario_id=scenario_id,
        scenario_key=str(row["scenario_key"]),
        name=str(row["name"]),
        description=None if row.get("description") is None else str(row["description"]),
        status=str(row["status"]),
        created_at=_datetime_or_none(row.get("created_at")),
        updated_at=_datetime_or_none(row.get("updated_at")),
        timeline=_timeline_from_row(timeline_rows[0]),
        network_settings=_network_settings_from_row(network_rows[0]),
        entities=tuple(_entity_from_row(item) for item in entity_rows),
        traffic_flows=tuple(_traffic_flow_from_row(item) for item in traffic_rows),
    )


def _scenario_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("scenario"), Mapping):
        nested = dict(payload["scenario"])
        for key in ("scenario_key", "name", "description", "status"):
            if key in payload and key not in nested:
                nested[key] = payload[key]
        return nested
    return dict(payload)


def _profile_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("profile payload must be an object")
    raw_params = payload.get("params")
    if raw_params is not None and not isinstance(raw_params, Mapping):
        raise ValueError("params must be an object")
    data = dict(raw_params or {})
    for key, value in payload.items():
        if key not in {"params", "metadata"}:
            data[str(key)] = value
    return data


def _profile_values(
    data: Mapping[str, Any],
    *,
    float_fields: tuple[str, ...],
    text_fields: tuple[str, ...],
    required_fields: tuple[str, ...],
    key_fields: Mapping[str, str],
) -> dict[str, Any]:
    allowed = set(_PROFILE_CONTROL_FIELDS) | set(float_fields) | set(text_fields)
    unknown = sorted(str(key) for key in data if str(key) not in allowed)
    if unknown:
        raise ValueError(
            "unsupported profile fields: "
            + ", ".join(unknown)
            + "; add explicit DB columns before accepting them"
        )
    missing = [
        field_name
        for field_name in required_fields
        if _profile_field_missing(field_name, data, key_fields)
    ]
    if missing:
        raise ValueError("profile is missing required calculation fields: " + ", ".join(missing))

    values: dict[str, Any] = {}
    for field_name in float_fields:
        values[field_name] = _optional_float(data.get(field_name))
    for field_name in text_fields:
        values[field_name] = _optional_str(data.get(field_name))
    return values


def _derive_relay_tx_power_dbw(data: dict[str, Any]) -> None:
    if data.get("tx_power_dbw") is not None:
        return
    required_fields = ("eirp_sat_dbw", "tx_antenna_gain_dbi", "tx_waveguide_loss_db")
    if any(data.get(field_name) is None for field_name in required_fields):
        return
    data["tx_power_dbw"] = (
        float(data["eirp_sat_dbw"])
        - float(data["tx_antenna_gain_dbi"])
        + float(data["tx_waveguide_loss_db"])
    )


def _profile_field_missing(
    field_name: str,
    data: Mapping[str, Any],
    key_fields: Mapping[str, str],
) -> bool:
    if field_name in key_fields:
        return not bool(_optional_str(key_fields[field_name]))
    value = data.get(field_name)
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _profile_metadata(payload: Mapping[str, Any]) -> dict[str, Any]:
    metadata = payload.get("metadata")
    if metadata is None:
        return {}
    if not isinstance(metadata, Mapping):
        raise ValueError("metadata must be an object")
    return dict(metadata)


def _timeline_from_payload(data: Mapping[str, Any]) -> ExplicitScenarioTimeline:
    timeline = dict(data.get("timeline", {})) if isinstance(data.get("timeline"), Mapping) else {}
    simulation = dict(data.get("simulation", {})) if isinstance(data.get("simulation"), Mapping) else {}
    start = timeline.get("start_time_utc", timeline.get("start_time", simulation.get("start_time")))
    end = timeline.get("end_time_utc", timeline.get("end_time", simulation.get("end_time")))
    step = timeline.get("time_step_sec", simulation.get("time_step_sec"))
    if step is None:
        raise ValueError("scenario timeline requires time_step_sec")
    return ExplicitScenarioTimeline(
        start_time_utc=_to_datetime_utc(start, "start_time_utc"),
        end_time_utc=_to_datetime_utc(end, "end_time_utc"),
        time_step_sec=int(step),
    )


def _network_settings_from_payload(data: Mapping[str, Any]) -> ExplicitScenarioNetworkSettings:
    settings = dict(data.get("network_settings", {})) if isinstance(data.get("network_settings"), Mapping) else {}
    simulation = dict(data.get("simulation", {})) if isinstance(data.get("simulation"), Mapping) else {}
    network = dict(data.get("network", {})) if isinstance(data.get("network"), Mapping) else {}
    selection_mode = _optional_str(settings.get("selection_mode", simulation.get("selection_mode")))
    earth_model = _optional_str(settings.get("earth_model", simulation.get("earth_model")))
    connectivity_mode = _optional_str(settings.get("connectivity_mode", network.get("connectivity_mode")))
    missing = [
        name
        for name, value in (
            ("selection_mode", selection_mode),
            ("earth_model", earth_model),
            ("connectivity_mode", connectivity_mode),
        )
        if value is None
    ]
    if missing:
        raise ValueError(f"scenario network settings require: {', '.join(missing)}")
    return ExplicitScenarioNetworkSettings(
        selection_mode=str(selection_mode),
        earth_model=str(earth_model),
        connectivity_mode=str(connectivity_mode),
        min_elevation_deg=_optional_float(settings.get("min_elevation_deg", network.get("min_elevation_deg"))),
        target_elevation_deg=_optional_float(
            settings.get("target_elevation_deg", network.get("target_elevation_deg"))
        ),
    )


def _entities_from_payload(data: Mapping[str, Any]) -> tuple[ExplicitScenarioEntity, ...]:
    raw_entities = data.get("entities")
    entities: list[ExplicitScenarioEntity] = []
    if raw_entities is not None:
        if not isinstance(raw_entities, list):
            raise ValueError("scenario entities must be an array")
        for index, raw_entity in enumerate(raw_entities):
            if not isinstance(raw_entity, Mapping):
                raise ValueError(f"scenario entities[{index}] must be an object")
            entities.append(_entity_from_payload(raw_entity, index))
        return tuple(entities)

    selection = dict(data.get("selection", {})) if isinstance(data.get("selection"), Mapping) else {}
    explicit_ids = selection.get("explicit_ids")
    if isinstance(explicit_ids, Mapping):
        for kind in REQUIRED_INVENTORY_KINDS:
            raw_values = explicit_ids.get(kind)
            if raw_values is None:
                continue
            if not isinstance(raw_values, list):
                raise ValueError(f"selection.explicit_ids.{kind} must be an array")
            for ordinal, raw_value in enumerate(raw_values):
                entity_key = _required_text(raw_value, f"selection.explicit_ids.{kind}[{ordinal}]")
                entities.append(
                    ExplicitScenarioEntity(
                        kind=kind,
                        entity_key=entity_key,
                        role=kind,
                        ordinal=ordinal,
                    )
                )
    raw_items = selection.get("items")
    if isinstance(raw_items, list):
        for index, raw_item in enumerate(raw_items):
            if not isinstance(raw_item, Mapping):
                raise ValueError(f"selection.items[{index}] must be an object")
            entities.append(_entity_from_payload(raw_item, index))
    return tuple(_deduplicate_entities(entities))


def _entity_from_payload(raw_entity: Mapping[str, Any], index: int) -> ExplicitScenarioEntity:
    kind = _required_str(raw_entity, "kind")
    if kind not in REQUIRED_INVENTORY_KINDS:
        raise ValueError(f"entity kind must be one of {sorted(REQUIRED_INVENTORY_KINDS)}")
    entity_key = _required_str(raw_entity, "entity_key")
    return ExplicitScenarioEntity(
        kind=kind,
        entity_key=entity_key,
        role=str(raw_entity.get("role") or kind),
        enabled=bool(raw_entity.get("enabled", True)),
        ordinal=int(raw_entity.get("ordinal", index)),
        ground_terminal_profile_key=_optional_str(raw_entity.get("ground_terminal_profile_key")),
        user_beam_profile_key=_optional_str(raw_entity.get("user_beam_profile_key")),
        feeder_beam_profile_key=_optional_str(raw_entity.get("feeder_beam_profile_key")),
    )


def _traffic_flows_from_payload(data: Mapping[str, Any]) -> tuple[ExplicitScenarioTrafficFlow, ...]:
    raw_flows = data.get("traffic_flows")
    if raw_flows is None and isinstance(data.get("traffic"), Mapping):
        raw_flows = data["traffic"].get("flows")
    if raw_flows is None:
        return ()
    if not isinstance(raw_flows, list):
        raise ValueError("scenario traffic flows must be an array")
    flows: list[ExplicitScenarioTrafficFlow] = []
    for index, raw_flow in enumerate(raw_flows):
        if not isinstance(raw_flow, Mapping):
            raise ValueError(f"scenario traffic_flows[{index}] must be an object")
        flow_key = _optional_str(raw_flow.get("flow_key", raw_flow.get("flow_id"))) or f"flow-{index}"
        flows.append(
            ExplicitScenarioTrafficFlow(
                flow_key=flow_key,
                source_kind=_required_str(raw_flow, "source_kind"),
                source_key=_required_str(raw_flow, "source_key"),
                target_kind=_required_str(raw_flow, "target_kind"),
                target_key=_required_str(raw_flow, "target_key"),
                requested_rate_mbps=float(raw_flow.get("requested_rate_mbps")),
                priority=int(raw_flow.get("priority", 100)),
                start_time_utc=_optional_datetime(raw_flow.get("start_time_utc"), "start_time_utc"),
                end_time_utc=_optional_datetime(raw_flow.get("end_time_utc"), "end_time_utc"),
            )
        )
    return tuple(flows)


def _deduplicate_entities(entities: list[ExplicitScenarioEntity]) -> list[ExplicitScenarioEntity]:
    by_key: dict[tuple[str, str], ExplicitScenarioEntity] = {}
    for entity in entities:
        by_key[(entity.kind, entity.entity_key)] = entity
    return sorted(by_key.values(), key=lambda item: (item.kind, item.ordinal, item.entity_key))


def _summary_from_row(row: Mapping[str, Any]) -> ExplicitScenarioSummary:
    return ExplicitScenarioSummary(
        scenario_id=int(row["scenario_id"]),
        scenario_key=str(row["scenario_key"]),
        name=str(row["name"]),
        description=None if row.get("description") is None else str(row["description"]),
        status=str(row["status"]),
        created_at=_datetime_or_none(row.get("created_at")),
        updated_at=_datetime_or_none(row.get("updated_at")),
    )


def _timeline_from_row(row: Mapping[str, Any]) -> ExplicitScenarioTimeline:
    return ExplicitScenarioTimeline(
        start_time_utc=_to_datetime_utc(row["start_time_utc"], "start_time_utc"),
        end_time_utc=_to_datetime_utc(row["end_time_utc"], "end_time_utc"),
        time_step_sec=int(row["time_step_sec"]),
    )


def _network_settings_from_row(row: Mapping[str, Any]) -> ExplicitScenarioNetworkSettings:
    return ExplicitScenarioNetworkSettings(
        selection_mode=str(row["selection_mode"]),
        earth_model=str(row["earth_model"]),
        connectivity_mode=str(row["connectivity_mode"]),
        min_elevation_deg=_optional_float(row.get("min_elevation_deg")),
        target_elevation_deg=_optional_float(row.get("target_elevation_deg")),
    )


def _entity_from_row(row: Mapping[str, Any]) -> ExplicitScenarioEntity:
    return ExplicitScenarioEntity(
        kind=str(row["kind"]),
        entity_key=str(row["entity_key"]),
        role=str(row["role"]),
        enabled=bool(row["enabled"]),
        ordinal=int(row["ordinal"]),
        ground_terminal_profile_key=_optional_str(row.get("ground_terminal_profile_key")),
        user_beam_profile_key=_optional_str(row.get("user_beam_profile_key")),
        feeder_beam_profile_key=_optional_str(row.get("feeder_beam_profile_key")),
    )


def _traffic_flow_from_row(row: Mapping[str, Any]) -> ExplicitScenarioTrafficFlow:
    return ExplicitScenarioTrafficFlow(
        flow_key=str(row["flow_key"]),
        source_kind=str(row["source_kind"]),
        source_key=str(row["source_key"]),
        target_kind=str(row["target_kind"]),
        target_key=str(row["target_key"]),
        requested_rate_mbps=float(row["requested_rate_mbps"]),
        priority=int(row["priority"]),
        start_time_utc=_datetime_or_none(row.get("start_time_utc")),
        end_time_utc=_datetime_or_none(row.get("end_time_utc")),
    )


def _scenario_identifier_condition(identifier: str | int) -> tuple[str, tuple[Any, ...]]:
    text = str(identifier).strip()
    if not text:
        raise ValueError("scenario identifier must not be empty")
    if text.isdecimal():
        return "scenario_id = %s", (int(text),)
    return "scenario_key = %s", (text,)


def _to_datetime_utc(value: Any, field_name: str) -> datetime:
    if value is None:
        raise ValueError(f"{field_name} is required")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError(f"{field_name} must be timezone-aware")
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        return _datetime_from_epoch_msec(int(value), field_name)
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    if re.fullmatch(r"[-+]?\d+", text):
        return _datetime_from_epoch_msec(int(text), field_name)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include timezone")
    return parsed.astimezone(timezone.utc)


def _datetime_from_epoch_msec(value: int, field_name: str) -> datetime:
    if abs(value) < 100_000_000_000:
        raise ValueError(f"{field_name} must be an absolute Unix timestamp in milliseconds")
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)


def _optional_datetime(value: Any, field_name: str) -> datetime | None:
    return None if value is None else _to_datetime_utc(value, field_name)


def _datetime_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    return _to_datetime_utc(value, "datetime")


def _time_to_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _required_str(data: Mapping[str, Any], field_name: str) -> str:
    return _required_text(data.get(field_name), field_name)


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    return text


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return None if not text else text


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _explicit_entity_field_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _entity_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _new_scenario_key(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if not slug:
        slug = "scenario"
    return f"{slug}-{uuid4().hex[:8]}"
