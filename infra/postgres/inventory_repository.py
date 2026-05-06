from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import uuid4

from simulation_core import constellation
from simulation_core.inventory_selection_spec import (
    InventorySelectionItem,
    InventorySelectionSpec,
)

from .inventory_store import (
    EXPLICIT_SCHEMA,
    INVENTORY_V2_SCHEMA,
    InventoryBundle,
    _connection_kwargs,
    _fetch_rows,
    _load_ground_terminal_profiles_v2,
    _load_relay_payload_profiles_v2,
    _merge_explicit_inventory_rows,
    _relation_exists,
    _require_psycopg,
)
from .inventory_v2_rebuild import ensure_schema_v2
from .settings import PostgresSettings, load_postgres_settings


@dataclass(slots=True)
class SelectionProfileDraft:
    name: str
    items: tuple[InventorySelectionItem, ...]
    profile_key: str | None = None
    description: str | None = None
    source_type: str = "manual"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SelectionProfileRecord:
    profile_id: int
    profile_key: str
    name: str
    description: str | None
    source_type: str
    is_active: bool
    metadata: dict[str, Any] = field(default_factory=dict)
    items: tuple[InventorySelectionItem, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_id": int(self.profile_id),
            "profile_key": str(self.profile_key),
            "name": str(self.name),
            "description": None if self.description is None else str(self.description),
            "source_type": str(self.source_type),
            "is_active": bool(self.is_active),
            "metadata": dict(self.metadata),
            "items": [item.to_dict() for item in self.items],
        }


@dataclass(slots=True)
class RunSnapshotRecord:
    snapshot_id: int
    request_key: str
    name: str
    selection_profile_id: int | None
    source_type: str
    status: str
    metadata: dict[str, Any] = field(default_factory=dict)
    items: tuple[InventorySelectionItem, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": int(self.snapshot_id),
            "request_key": str(self.request_key),
            "name": str(self.name),
            "selection_profile_id": None
            if self.selection_profile_id is None
            else int(self.selection_profile_id),
            "source_type": str(self.source_type),
            "status": str(self.status),
            "metadata": dict(self.metadata),
            "items": [item.to_dict() for item in self.items],
        }


@dataclass(slots=True)
class SelectionCatalogRecord:
    catalog_id: int
    kind: str
    catalog_key: str
    name: str | None
    description: str | None
    source_type: str
    member_count: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "catalog_id": int(self.catalog_id),
            "kind": str(self.kind),
            "catalog_key": str(self.catalog_key),
            "name": None if self.name is None else str(self.name),
            "description": None if self.description is None else str(self.description),
            "source_type": str(self.source_type),
            "member_count": int(self.member_count),
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class SelectionProfileSummary:
    profile_id: int
    profile_key: str
    name: str
    description: str | None
    source_type: str
    is_active: bool
    item_count: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_id": int(self.profile_id),
            "profile_key": str(self.profile_key),
            "name": str(self.name),
            "description": None if self.description is None else str(self.description),
            "source_type": str(self.source_type),
            "is_active": bool(self.is_active),
            "item_count": int(self.item_count),
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class InventoryEntityRecord:
    kind: str
    entity_key: str
    name: str
    lat: float | None = None
    lon: float | None = None
    alt_m: float | None = None
    radio_profile: str | None = None
    ground_terminal_profile_key: str | None = None
    user_beam_profile_key: str | None = None
    feeder_beam_profile_key: str | None = None
    role: str | None = None
    region_code: int | None = None
    frequency: int | None = None
    priority: float | None = None
    subject_code: int | None = None
    subject_name: str | None = None
    federal_district: str | None = None
    grid_cell_id: str | None = None
    seed_version: str | None = None
    is_active: bool | None = None
    catalog_ordinal: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": str(self.kind),
            "entity_key": str(self.entity_key),
            "name": str(self.name),
            "lat": None if self.lat is None else float(self.lat),
            "lon": None if self.lon is None else float(self.lon),
            "alt_m": None if self.alt_m is None else float(self.alt_m),
            "radio_profile": None if self.radio_profile is None else str(self.radio_profile),
            "ground_terminal_profile_key": None
            if self.ground_terminal_profile_key is None
            else str(self.ground_terminal_profile_key),
            "user_beam_profile_key": None
            if self.user_beam_profile_key is None
            else str(self.user_beam_profile_key),
            "feeder_beam_profile_key": None
            if self.feeder_beam_profile_key is None
            else str(self.feeder_beam_profile_key),
            "role": None if self.role is None else str(self.role),
            "region_code": None if self.region_code is None else int(self.region_code),
            "frequency": None if self.frequency is None else int(self.frequency),
            "priority": None if self.priority is None else float(self.priority),
            "subject_code": None if self.subject_code is None else int(self.subject_code),
            "subject_name": None if self.subject_name is None else str(self.subject_name),
            "federal_district": None if self.federal_district is None else str(self.federal_district),
            "grid_cell_id": None if self.grid_cell_id is None else str(self.grid_cell_id),
            "seed_version": None if self.seed_version is None else str(self.seed_version),
            "is_active": None if self.is_active is None else bool(self.is_active),
            "catalog_ordinal": None if self.catalog_ordinal is None else int(self.catalog_ordinal),
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class InventoryEntitySearchPage:
    kind: str
    items: tuple[InventoryEntityRecord, ...]
    total_count: int
    limit: int
    offset: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": str(self.kind),
            "items": [item.to_dict() for item in self.items],
            "total_count": int(self.total_count),
            "limit": int(self.limit),
            "offset": int(self.offset),
        }


@dataclass(slots=True)
class InventoryEntityCoverageBucket:
    lat_bucket: int
    lon_bucket: int
    count: int
    lat: float
    lon: float
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "lat_bucket": int(self.lat_bucket),
            "lon_bucket": int(self.lon_bucket),
            "count": int(self.count),
            "lat": float(self.lat),
            "lon": float(self.lon),
            "lat_min": float(self.lat_min),
            "lat_max": float(self.lat_max),
            "lon_min": float(self.lon_min),
            "lon_max": float(self.lon_max),
        }


@dataclass(slots=True)
class InventoryEntityCoverage:
    kind: str
    total_count: int
    coord_count: int
    lat_bins: int
    lon_bins: int
    bounds: dict[str, float] = field(default_factory=dict)
    buckets: tuple[InventoryEntityCoverageBucket, ...] = ()
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": str(self.kind),
            "total_count": int(self.total_count),
            "coord_count": int(self.coord_count),
            "lat_bins": int(self.lat_bins),
            "lon_bins": int(self.lon_bins),
            "bounds": dict(self.bounds),
            "buckets": [bucket.to_dict() for bucket in self.buckets],
            "note": self.note,
        }


@dataclass(slots=True)
class InventoryEntityWriteDraft:
    kind: str
    entity_key: str
    name: str
    values: dict[str, Any]
    catalog_key: str
    catalog_name: str | None = None
    catalog_description: str | None = None
    catalog_ordinal: int = 0
    source_type: str = "api"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> InventoryEntityRecord:
        values = dict(self.values)
        return InventoryEntityRecord(
            kind=self.kind,
            entity_key=self.entity_key,
            name=self.name,
            lat=values.get("lat"),
            lon=values.get("lon"),
            alt_m=values.get("site_alt_m", values.get("alt_m")),
            radio_profile=values.get("radio_profile"),
            ground_terminal_profile_key=values.get("ground_terminal_profile_key"),
            user_beam_profile_key=values.get("user_beam_profile_key"),
            feeder_beam_profile_key=values.get("feeder_beam_profile_key"),
            role=values.get("role"),
            region_code=values.get("region_code"),
            frequency=values.get("frequency"),
            priority=values.get("priority"),
            subject_code=values.get("subject_code"),
            subject_name=values.get("subject_name"),
            federal_district=values.get("federal_district"),
            grid_cell_id=values.get("grid_cell_id"),
            seed_version=values.get("seed_version"),
            is_active=values.get("is_active", True),
            catalog_ordinal=self.catalog_ordinal,
            metadata=dict(self.metadata),
        )


@dataclass(slots=True)
class GroundTerminalProfileRecord:
    profile_key: str
    station_kind: str
    params: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_key": str(self.profile_key),
            "station_kind": str(self.station_kind),
            "params": dict(self.params),
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class RelayPayloadProfileRecord:
    profile_key: str
    relay_mode: str
    params: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_key": str(self.profile_key),
            "relay_mode": str(self.relay_mode),
            "params": dict(self.params),
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class FrequencyChannelRecord:
    channel_index: int
    center_frequency_ghz: float
    bandwidth_mhz: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "channel_index": int(self.channel_index),
            "center_frequency_ghz": float(self.center_frequency_ghz),
            "bandwidth_mhz": float(self.bandwidth_mhz),
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class FrequencyPlanRecord:
    plan_key: str
    name: str
    description: str | None
    metadata: dict[str, Any] = field(default_factory=dict)
    channels: tuple[FrequencyChannelRecord, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_key": str(self.plan_key),
            "name": str(self.name),
            "description": None if self.description is None else str(self.description),
            "metadata": dict(self.metadata),
            "channels": [channel.to_dict() for channel in self.channels],
        }


@dataclass(slots=True)
class SatelliteConstellationRecord:
    constellation_key: str
    name: str
    params: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    satellite_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "constellation_key": str(self.constellation_key),
            "name": str(self.name),
            "params": dict(self.params),
            "metadata": dict(self.metadata),
            "satellite_count": int(self.satellite_count),
        }


@dataclass(slots=True)
class InventoryProfileAssignmentResult:
    kind: str
    updated_count: int
    entity_keys: tuple[str, ...]
    ground_terminal_profile_key: str | None = None
    user_beam_profile_key: str | None = None
    feeder_beam_profile_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": str(self.kind),
            "updated_count": int(self.updated_count),
            "entity_keys": [str(item) for item in self.entity_keys],
            "ground_terminal_profile_key": None
            if self.ground_terminal_profile_key is None
            else str(self.ground_terminal_profile_key),
            "user_beam_profile_key": None
            if self.user_beam_profile_key is None
            else str(self.user_beam_profile_key),
            "feeder_beam_profile_key": None
            if self.feeder_beam_profile_key is None
            else str(self.feeder_beam_profile_key),
        }


@dataclass(slots=True)
class InventoryProfileAssignmentRuleRecord:
    rule_id: int
    rule_key: str
    name: str
    kind: str
    catalog_key: str | None = None
    query: str | None = None
    bbox: dict[str, float] | None = None
    region_codes: tuple[int, ...] = field(default_factory=tuple)
    subject_codes: tuple[int, ...] = field(default_factory=tuple)
    federal_districts: tuple[str, ...] = field(default_factory=tuple)
    grid_cell_ids: tuple[str, ...] = field(default_factory=tuple)
    is_active_filter: bool | None = None
    ground_terminal_profile_key: str | None = None
    user_beam_profile_key: str | None = None
    feeder_beam_profile_key: str | None = None
    priority: int = 100
    is_active: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": int(self.rule_id),
            "rule_key": str(self.rule_key),
            "name": str(self.name),
            "kind": str(self.kind),
            "catalog_key": None if self.catalog_key is None else str(self.catalog_key),
            "query": None if self.query is None else str(self.query),
            "bbox": None if self.bbox is None else {str(key): float(value) for key, value in self.bbox.items()},
            "region_codes": [int(value) for value in self.region_codes],
            "subject_codes": [int(value) for value in self.subject_codes],
            "federal_districts": [str(value) for value in self.federal_districts],
            "grid_cell_ids": [str(value) for value in self.grid_cell_ids],
            "is_active_filter": None if self.is_active_filter is None else bool(self.is_active_filter),
            "ground_terminal_profile_key": None
            if self.ground_terminal_profile_key is None
            else str(self.ground_terminal_profile_key),
            "user_beam_profile_key": None
            if self.user_beam_profile_key is None
            else str(self.user_beam_profile_key),
            "feeder_beam_profile_key": None
            if self.feeder_beam_profile_key is None
            else str(self.feeder_beam_profile_key),
            "priority": int(self.priority),
            "is_active": bool(self.is_active),
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class InventoryProfileAssignmentRulePreview:
    rule: InventoryProfileAssignmentRuleRecord
    matched_count: int
    entity_keys_preview: tuple[str, ...] = field(default_factory=tuple)
    truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule.to_dict(),
            "matched_count": int(self.matched_count),
            "entity_keys_preview": [str(item) for item in self.entity_keys_preview],
            "truncated": bool(self.truncated),
        }


@dataclass(slots=True)
class InventoryProfileRuleApplicationResult:
    rule: InventoryProfileAssignmentRuleRecord
    assignment: InventoryProfileAssignmentResult

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule.to_dict(),
            "assignment": self.assignment.to_dict(),
        }


@dataclass(slots=True)
class InventoryProfileRuleBatchResult:
    results: tuple[InventoryProfileRuleApplicationResult, ...] = field(default_factory=tuple)
    total_updated_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "results": [item.to_dict() for item in self.results],
            "total_updated_count": int(self.total_updated_count),
        }


_KIND_ENTITY_CONFIG = {
    "gateways": {
        "table": "gateway",
        "entity_id_column": "gateway_id",
        "key_column": "gateway_key",
        "member_table": "gateway_catalog_member",
        "member_entity_column": "gateway_id",
        "search_select": """
            e.gateway_key AS entity_key,
            e.name,
            e.lat,
            e.lon,
            e.site_alt_m AS alt_m,
            COALESCE(e.radio_profile, 'default') AS radio_profile,
            COALESCE(e.ground_terminal_profile_key, e.radio_profile, 'default') AS ground_terminal_profile_key,
            NULL::text AS user_beam_profile_key,
            NULL::text AS feeder_beam_profile_key,
            COALESCE(e.role, 'gateway') AS role,
            NULL::integer AS region_code,
            NULL::integer AS frequency,
            NULL::double precision AS priority,
            NULL::integer AS subject_code,
            NULL::text AS subject_name,
            NULL::text AS federal_district,
            NULL::text AS grid_cell_id,
            NULL::text AS seed_version,
            NULL::boolean AS is_active,
            COALESCE(e.metadata, '{}'::jsonb) AS metadata
        """,
        "select": """
            SELECT g.gateway_key AS id, g.name, g.lat, g.lon, g.site_alt_m, g.antenna_height_agl_m,
                   COALESCE(g.radio_profile, 'default') AS radio_profile,
                   COALESCE(g.ground_terminal_profile_key, g.radio_profile, 'default') AS ground_terminal_profile_key,
                   COALESCE(g.role, 'gateway') AS role,
                   COALESCE(g.limits, '{}'::jsonb) AS limits,
                   COALESCE(g.metadata, '{}'::jsonb) AS metadata
            FROM selected
            JOIN inventory_v2.gateway g ON g.gateway_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
    "targets": {
        "table": "target",
        "entity_id_column": "target_id",
        "key_column": "target_key",
        "member_table": "target_catalog_member",
        "member_entity_column": "target_id",
        "search_select": """
            e.target_key AS entity_key,
            e.name,
            e.lat,
            e.lon,
            e.site_alt_m AS alt_m,
            NULL::text AS radio_profile,
            e.ground_terminal_profile_key,
            NULL::text AS user_beam_profile_key,
            NULL::text AS feeder_beam_profile_key,
            NULL::text AS role,
            NULL::integer AS region_code,
            e.frequency,
            e.priority,
            NULL::integer AS subject_code,
            NULL::text AS subject_name,
            NULL::text AS federal_district,
            NULL::text AS grid_cell_id,
            NULL::text AS seed_version,
            NULL::boolean AS is_active,
            COALESCE(e.metadata, '{}'::jsonb) AS metadata
        """,
        "select": """
            SELECT t.target_key AS id, t.name, t.lat, t.lon, t.frequency, t.priority,
                   t.site_alt_m, t.antenna_height_agl_m,
                   t.ground_terminal_profile_key,
                   COALESCE(t.metadata, '{}'::jsonb) AS metadata
            FROM selected
            JOIN inventory_v2.target t ON t.target_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
    "satellites": {
        "table": "satellite",
        "entity_id_column": "satellite_id",
        "key_column": "satellite_key",
        "member_table": "satellite_catalog_member",
        "member_entity_column": "satellite_id",
        "search_select": """
            e.satellite_key AS entity_key,
            e.name,
            NULL::double precision AS lat,
            NULL::double precision AS lon,
            NULL::double precision AS alt_m,
            COALESCE(e.radio_profile, 'default') AS radio_profile,
            NULL::text AS ground_terminal_profile_key,
            COALESCE(e.user_beam_profile_key, e.radio_profile, 'default') AS user_beam_profile_key,
            COALESCE(e.feeder_beam_profile_key, e.radio_profile, 'default') AS feeder_beam_profile_key,
            NULL::text AS role,
            NULL::integer AS region_code,
            NULL::integer AS frequency,
            NULL::double precision AS priority,
            NULL::integer AS subject_code,
            NULL::text AS subject_name,
            NULL::text AS federal_district,
            NULL::text AS grid_cell_id,
            NULL::text AS seed_version,
            NULL::boolean AS is_active,
            COALESCE(e.metadata, '{}'::jsonb) AS metadata
        """,
        "select": """
            SELECT s.satellite_key AS id, s.name, s.tle_line1, s.tle_line2,
                   COALESCE(s.radio_profile, 'default') AS radio_profile,
                   COALESCE(s.user_beam_profile_key, s.radio_profile, 'default') AS user_beam_profile_key,
                   COALESCE(s.feeder_beam_profile_key, s.radio_profile, 'default') AS feeder_beam_profile_key,
                   s.connection_min, s.beam_layout_mode, s.dynamic_ray_count,
                   s.dynamic_ray_aperture_deg, s.sat_haps_ray_count,
                   COALESCE(s.resource_limits, '{}'::jsonb) AS resource_limits,
                   COALESCE(s.allowed_link_types, '[]'::jsonb) AS allowed_link_types,
                   COALESCE(s.metadata, '{}'::jsonb) AS metadata
            FROM selected
            JOIN inventory_v2.satellite s ON s.satellite_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
    "haps": {
        "table": "haps",
        "entity_id_column": "haps_id",
        "key_column": "haps_key",
        "member_table": "haps_catalog_member",
        "member_entity_column": "haps_id",
        "search_select": """
            e.haps_key AS entity_key,
            e.name,
            e.lat,
            e.lon,
            e.alt_m AS alt_m,
            COALESCE(e.radio_profile, 'default') AS radio_profile,
            NULL::text AS ground_terminal_profile_key,
            COALESCE(e.user_beam_profile_key, e.radio_profile, 'default') AS user_beam_profile_key,
            COALESCE(e.feeder_beam_profile_key, e.radio_profile, 'default') AS feeder_beam_profile_key,
            NULL::text AS role,
            NULL::integer AS region_code,
            NULL::integer AS frequency,
            NULL::double precision AS priority,
            NULL::integer AS subject_code,
            NULL::text AS subject_name,
            NULL::text AS federal_district,
            NULL::text AS grid_cell_id,
            NULL::text AS seed_version,
            NULL::boolean AS is_active,
            COALESCE(e.metadata, '{}'::jsonb) AS metadata
        """,
        "select": """
            SELECT h.haps_key AS id, h.name, h.lat, h.lon, h.alt_m,
                   COALESCE(h.radio_profile, 'default') AS radio_profile,
                   COALESCE(h.user_beam_profile_key, h.radio_profile, 'default') AS user_beam_profile_key,
                   COALESCE(h.feeder_beam_profile_key, h.radio_profile, 'default') AS feeder_beam_profile_key,
                   h.connection_min,
                   COALESCE(h.resource_limits, '{}'::jsonb) AS resource_limits,
                   COALESCE(h.metadata, '{}'::jsonb) AS metadata
            FROM selected
            JOIN inventory_v2.haps h ON h.haps_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
    "vsats": {
        "table": "vsat",
        "entity_id_column": "vsat_id",
        "key_column": "vsat_key",
        "member_table": "vsat_catalog_member",
        "member_entity_column": "vsat_id",
        "search_select": """
            e.vsat_key AS entity_key,
            e.name,
            e.lat,
            e.lon,
            e.site_alt_m AS alt_m,
            COALESCE(e.radio_profile, 'default') AS radio_profile,
            COALESCE(e.ground_terminal_profile_key, e.radio_profile, 'default') AS ground_terminal_profile_key,
            NULL::text AS user_beam_profile_key,
            NULL::text AS feeder_beam_profile_key,
            COALESCE(e.role, 'vsat') AS role,
            e.region_code,
            NULL::integer AS frequency,
            NULL::double precision AS priority,
            NULL::integer AS subject_code,
            NULL::text AS subject_name,
            NULL::text AS federal_district,
            NULL::text AS grid_cell_id,
            NULL::text AS seed_version,
            NULL::boolean AS is_active,
            COALESCE(e.metadata, '{}'::jsonb) AS metadata
        """,
        "select": """
            SELECT v.vsat_key AS id, v.name, v.lat, v.lon, v.region_code,
                   v.site_alt_m, v.antenna_height_agl_m,
                   COALESCE(v.radio_profile, 'default') AS radio_profile,
                   COALESCE(v.ground_terminal_profile_key, v.radio_profile, 'default') AS ground_terminal_profile_key,
                   COALESCE(v.role, 'vsat') AS role,
                   COALESCE(v.limits, '{}'::jsonb) AS limits,
                   COALESCE(v.metadata, '{}'::jsonb) AS metadata
            FROM selected
            JOIN inventory_v2.vsat v ON v.vsat_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
    "theoretical_subscribers": {
        "table": "theoretical_subscriber",
        "entity_id_column": "theoretical_subscriber_id",
        "key_column": "subscriber_key",
        "member_table": "theoretical_subscriber_catalog_member",
        "member_entity_column": "theoretical_subscriber_id",
        "search_select": """
            e.subscriber_key AS entity_key,
            e.name,
            e.lat,
            e.lon,
            e.site_alt_m AS alt_m,
            COALESCE(e.ground_terminal_profile_key, 'default') AS radio_profile,
            COALESCE(e.ground_terminal_profile_key, 'default') AS ground_terminal_profile_key,
            NULL::text AS user_beam_profile_key,
            NULL::text AS feeder_beam_profile_key,
            NULL::text AS role,
            NULL::integer AS region_code,
            NULL::integer AS frequency,
            NULL::double precision AS priority,
            e.subject_code,
            e.subject_name,
            e.federal_district,
            e.grid_cell_id,
            e.seed_version,
            e.is_active,
            COALESCE(e.metadata, '{}'::jsonb) AS metadata
        """,
        "select": """
            SELECT t.subscriber_key AS id, t.name, t.lat, t.lon,
                   t.site_alt_m, COALESCE(t.ground_terminal_profile_key, 'default') AS ground_terminal_profile_key,
                   t.subject_code, t.subject_name,
                   t.federal_district, t.grid_cell_id, t.seed_version,
                   t.is_active, COALESCE(t.metadata, '{}'::jsonb) AS metadata
            FROM selected
            JOIN inventory_v2.theoretical_subscriber t ON t.subscriber_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
}

_EXPLICIT_ENTITY_CONFIG = {
    "gateways": {
        "table": "gateway",
        "key_column": "gateway_key",
        "storage_model": f"{EXPLICIT_SCHEMA}.gateway",
        "limits_fields": ("connect_limit", "capacity_mbps", "bandwidth_mhz", "spectral_efficiency_bps_hz"),
        "select": """
            SELECT g.gateway_key AS id, g.name, g.lat, g.lon, g.site_alt_m, g.antenna_height_agl_m,
                   COALESCE(g.radio_profile, 'default') AS radio_profile,
                   COALESCE(g.ground_terminal_profile_key, g.radio_profile, 'default') AS ground_terminal_profile_key,
                   COALESCE(g.role, 'gateway') AS role,
                   g.connect_limit, g.capacity_mbps, g.bandwidth_mhz, g.spectral_efficiency_bps_hz,
                   g.source_name
            FROM selected
            JOIN inventory_explicit.gateway g ON g.gateway_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
    "targets": {
        "table": "target",
        "key_column": "target_key",
        "storage_model": f"{EXPLICIT_SCHEMA}.target",
        "limits_fields": (),
        "select": """
            SELECT t.target_key AS id, t.name, t.lat, t.lon, t.frequency, t.priority,
                   t.site_alt_m, t.antenna_height_agl_m,
                   t.ground_terminal_profile_key,
                   t.source_name
            FROM selected
            JOIN inventory_explicit.target t ON t.target_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
    "satellites": {
        "table": "satellite",
        "key_column": "satellite_key",
        "storage_model": f"{EXPLICIT_SCHEMA}.satellite",
        "limits_fields": (),
        "select": """
            SELECT s.satellite_key AS id, s.name, s.tle_line1, s.tle_line2,
                   COALESCE(s.radio_profile, 'default') AS radio_profile,
                   COALESCE(s.user_beam_profile_key, s.radio_profile, 'default') AS user_beam_profile_key,
                   COALESCE(s.feeder_beam_profile_key, s.radio_profile, 'default') AS feeder_beam_profile_key,
                   s.connection_min, s.beam_layout_mode, s.dynamic_ray_count,
                   s.dynamic_ray_aperture_deg, s.sat_haps_ray_count,
                   '{}'::jsonb AS resource_limits,
                   '[]'::jsonb AS allowed_link_types,
                   s.source_name
            FROM selected
            JOIN inventory_explicit.satellite s ON s.satellite_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
    "haps": {
        "table": "haps",
        "key_column": "haps_key",
        "storage_model": f"{EXPLICIT_SCHEMA}.haps",
        "limits_fields": (),
        "select": """
            SELECT h.haps_key AS id, h.name, h.lat, h.lon, h.alt_m,
                   COALESCE(h.radio_profile, 'default') AS radio_profile,
                   COALESCE(h.user_beam_profile_key, h.radio_profile, 'default') AS user_beam_profile_key,
                   COALESCE(h.feeder_beam_profile_key, h.radio_profile, 'default') AS feeder_beam_profile_key,
                   h.connection_min,
                   '{}'::jsonb AS resource_limits,
                   h.source_name
            FROM selected
            JOIN inventory_explicit.haps h ON h.haps_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
    "vsats": {
        "table": "vsat",
        "key_column": "vsat_key",
        "storage_model": f"{EXPLICIT_SCHEMA}.vsat",
        "limits_fields": ("connect_limit",),
        "select": """
            SELECT v.vsat_key AS id, v.name, v.lat, v.lon, v.region_code,
                   v.site_alt_m, v.antenna_height_agl_m,
                   COALESCE(v.radio_profile, 'default') AS radio_profile,
                   COALESCE(v.ground_terminal_profile_key, v.radio_profile, 'default') AS ground_terminal_profile_key,
                   COALESCE(v.role, 'vsat') AS role,
                   v.connect_limit,
                   v.source_name
            FROM selected
            JOIN inventory_explicit.vsat v ON v.vsat_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
    "theoretical_subscribers": {
        "table": "theoretical_subscriber",
        "key_column": "subscriber_key",
        "storage_model": f"{EXPLICIT_SCHEMA}.theoretical_subscriber",
        "limits_fields": (),
        "select": """
            SELECT t.subscriber_key AS id, t.name, t.lat, t.lon,
                   t.site_alt_m, COALESCE(t.ground_terminal_profile_key, 'default') AS ground_terminal_profile_key,
                   t.subject_code, t.subject_name,
                   t.federal_district, t.grid_cell_id, t.seed_version,
                   t.is_active, t.source_name
            FROM selected
            JOIN inventory_explicit.theoretical_subscriber t ON t.subscriber_key = selected.entity_key
            ORDER BY selected.ordinal
        """,
    },
}


def _require_psycopg_with_jsonb() -> tuple[Any, Any]:
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise RuntimeError(
            "Inventory PostgreSQL support requires the 'psycopg' package. "
            "Install dependencies from requirements.txt before working with selection profiles."
        ) from exc
    return psycopg, Jsonb


def _ordered_items(items: tuple[InventorySelectionItem, ...]) -> tuple[InventorySelectionItem, ...]:
    return tuple(
        sorted(
            items,
            key=lambda item: (
                str(item.kind),
                int(item.ordinal),
                str(item.entity_key),
            ),
        )
    )


def _items_from_rows(rows: list[dict[str, Any]], *, include_enabled: bool) -> tuple[InventorySelectionItem, ...]:
    items: list[InventorySelectionItem] = []
    for row in rows:
        items.append(
            InventorySelectionItem(
                kind=str(row["kind"]),
                entity_key=str(row["entity_key"]),
                enabled=True if not include_enabled else bool(row.get("enabled", True)),
                ordinal=int(row.get("ordinal", 0)),
                metadata=dict(row.get("metadata", {})) if isinstance(row.get("metadata"), dict) else {},
            )
        )
    return _ordered_items(tuple(items))


def _profile_items(cursor: Any, profile_id: int) -> tuple[InventorySelectionItem, ...]:
    rows = _fetch_rows(
        cursor,
        f"""
        SELECT kind, entity_key, enabled, ordinal, metadata
        FROM {INVENTORY_V2_SCHEMA}.selection_profile_item
        WHERE profile_id = %s
        ORDER BY kind, ordinal, entity_key
        """,
        (int(profile_id),),
    )
    return _items_from_rows(rows, include_enabled=True)


def _snapshot_items(cursor: Any, snapshot_id: int) -> tuple[InventorySelectionItem, ...]:
    rows = _fetch_rows(
        cursor,
        f"""
        SELECT kind, entity_key, ordinal, metadata
        FROM {INVENTORY_V2_SCHEMA}.simulation_request_snapshot_item
        WHERE snapshot_id = %s
        ORDER BY kind, ordinal, entity_key
        """,
        (int(snapshot_id),),
    )
    return _items_from_rows(rows, include_enabled=False)


def _selection_items(cursor: Any, selection: InventorySelectionSpec) -> tuple[InventorySelectionItem, ...]:
    if selection.run_snapshot_id is not None:
        return _snapshot_items(cursor, selection.run_snapshot_id)
    if selection.selection_profile_id is not None:
        return tuple(item for item in _profile_items(cursor, selection.selection_profile_id) if bool(item.enabled))
    return tuple(item for item in selection.items if bool(item.enabled))


def _kind_key_lists(items: tuple[InventorySelectionItem, ...]) -> dict[str, list[str]]:
    keys_by_kind = {kind: [] for kind in _KIND_ENTITY_CONFIG}
    for item in items:
        kind = str(item.kind)
        entity_key = str(item.entity_key)
        if kind not in keys_by_kind or not entity_key:
            continue
        if entity_key in keys_by_kind[kind]:
            continue
        keys_by_kind[kind].append(entity_key)
    return keys_by_kind


def _load_rows_for_keys(cursor: Any, kind: str, keys: list[str]) -> list[dict[str, Any]]:
    if not keys:
        return []
    config = _KIND_ENTITY_CONFIG[kind]
    query = f"""
        WITH selected(entity_key, ordinal) AS (
            SELECT * FROM unnest(%s::text[]) WITH ORDINALITY
        )
        {config["select"]}
    """
    rows = _fetch_rows(cursor, query, (list(keys),))
    resolved = {str(row["id"]) for row in rows}
    missing = [key for key in keys if key not in resolved]
    if missing:
        raise ValueError(f"Inventory selection for '{kind}' contains unknown entity keys: {missing}")
    return rows


def _load_explicit_rows_for_keys(cursor: Any, kind: str, keys: list[str]) -> list[dict[str, Any]]:
    if not keys:
        return []
    config = _EXPLICIT_ENTITY_CONFIG[kind]
    if not _relation_exists(cursor, f"{EXPLICIT_SCHEMA}.{config['table']}"):
        return _mark_entity_storage_rows(
            _load_rows_for_keys(cursor, kind, keys),
            f"{INVENTORY_V2_SCHEMA}.{_KIND_ENTITY_CONFIG[kind]['table']}",
        )
    query = f"""
        WITH selected(entity_key, ordinal) AS (
            SELECT * FROM unnest(%s::text[]) WITH ORDINALITY
        )
        {config["select"]}
    """
    explicit_rows = _fetch_rows(cursor, query, (list(keys),))
    explicit_rows = _prepare_explicit_entity_rows(explicit_rows, config)
    by_id = {str(row["id"]): row for row in explicit_rows}
    missing = [key for key in keys if key not in by_id]
    if missing:
        fallback_rows = _mark_entity_storage_rows(
            _load_rows_for_keys(cursor, kind, missing),
            f"{INVENTORY_V2_SCHEMA}.{_KIND_ENTITY_CONFIG[kind]['table']}",
        )
        by_id.update({str(row["id"]): row for row in fallback_rows})
    return [by_id[key] for key in keys if key in by_id]


def _prepare_explicit_entity_rows(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    limits_fields = tuple(config.get("limits_fields", ()))
    storage_model = str(config["storage_model"])
    for row in rows:
        item = dict(row)
        source_name = item.pop("source_name", None)
        if limits_fields:
            for field_name in limits_fields:
                value = item.get(field_name)
                if value is None:
                    item.pop(field_name, None)
        metadata = {"entity_storage_model": storage_model}
        if source_name is not None:
            metadata["source_name"] = str(source_name)
        item["metadata"] = metadata
        prepared.append(item)
    return prepared


def _mark_entity_storage_rows(rows: list[dict[str, Any]], storage_model: str) -> list[dict[str, Any]]:
    marked: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        metadata = dict(item.get("metadata", {})) if isinstance(item.get("metadata"), dict) else {}
        metadata["entity_storage_model"] = str(storage_model)
        item["metadata"] = metadata
        marked.append(item)
    return marked


def _load_rows_for_selection_source(
    cursor: Any,
    kind: str,
    keys: list[str],
    source: str,
) -> list[dict[str, Any]]:
    if source == "inventory_explicit":
        return _load_explicit_rows_for_keys(cursor, kind, keys)
    return _load_rows_for_keys(cursor, kind, keys)


def _bundle_from_items(
    cursor: Any,
    items: tuple[InventorySelectionItem, ...],
    *,
    source: str = "postgres_v2",
) -> InventoryBundle:
    keys_by_kind = _kind_key_lists(items)
    satellites = _load_rows_for_selection_source(cursor, "satellites", keys_by_kind["satellites"], source)
    haps = _load_rows_for_selection_source(cursor, "haps", keys_by_kind["haps"], source)
    _merge_explicit_inventory_rows(cursor, satellites=satellites, haps=haps)
    gateways = _load_rows_for_selection_source(cursor, "gateways", keys_by_kind["gateways"], source)
    targets = _load_rows_for_selection_source(cursor, "targets", keys_by_kind["targets"], source)
    vsats = _load_rows_for_selection_source(cursor, "vsats", keys_by_kind["vsats"], source)
    theoretical_subscribers = _load_rows_for_selection_source(
        cursor,
        "theoretical_subscribers",
        keys_by_kind["theoretical_subscribers"],
        source,
    )
    _apply_selection_item_profile_overrides(
        items,
        gateways=gateways,
        targets=targets,
        satellites=satellites,
        haps=haps,
        vsats=vsats,
        theoretical_subscribers=theoretical_subscribers,
    )
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


def _apply_selection_item_profile_overrides(
    items: tuple[InventorySelectionItem, ...],
    *,
    gateways: list[dict[str, Any]],
    targets: list[dict[str, Any]],
    satellites: list[dict[str, Any]],
    haps: list[dict[str, Any]],
    vsats: list[dict[str, Any]],
    theoretical_subscribers: list[dict[str, Any]],
) -> None:
    rows_by_kind = {
        "gateways": gateways,
        "targets": targets,
        "satellites": satellites,
        "haps": haps,
        "vsats": vsats,
        "theoretical_subscribers": theoretical_subscribers,
    }
    metadata_by_key = {
        (str(item.kind), str(item.entity_key)): dict(item.metadata)
        for item in items
        if item.metadata
    }
    for kind, rows in rows_by_kind.items():
        for row in rows:
            overrides = metadata_by_key.get((kind, str(row["id"])), {})
            if not overrides:
                continue
            _apply_profile_override(row, overrides, "ground_terminal_profile_key")
            _apply_profile_override(row, overrides, "user_beam_profile_key")
            _apply_profile_override(row, overrides, "feeder_beam_profile_key")


def _apply_profile_override(row: dict[str, Any], overrides: Mapping[str, Any], field_name: str) -> None:
    value = overrides.get(field_name)
    if value is None:
        return
    text = str(value).strip()
    if text:
        row[field_name] = text


def _normalize_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ValueError("list filter must be an array of values")


def _normalize_bbox(value: Any) -> dict[str, float] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("bbox filter must be an object")
    required = ("lat_min", "lat_max", "lon_min", "lon_max")
    if any(key not in value for key in required):
        raise ValueError("bbox filter must define lat_min, lat_max, lon_min and lon_max")
    return {
        "lat_min": float(value["lat_min"]),
        "lat_max": float(value["lat_max"]),
        "lon_min": float(value["lon_min"]),
        "lon_max": float(value["lon_max"]),
    }


def _search_query_parts(
    normalized_kind: str,
    config: dict[str, Any],
    *,
    query: str | None,
    bbox: dict[str, float] | None,
    region_codes: list[int] | None,
    subject_codes: list[int] | None,
    federal_districts: list[str] | None,
    grid_cell_ids: list[str] | None,
    is_active: bool | None,
) -> tuple[str, list[Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []

    normalized_query = str(query or "").strip().lower()
    if normalized_query:
        where_clauses.append(
            f"(LOWER(e.name) LIKE %s OR LOWER(e.{config['key_column']}) LIKE %s)"
        )
        like = f"%{normalized_query}%"
        params.extend((like, like))

    if bbox is not None:
        if normalized_kind not in {"vsats", "theoretical_subscribers"}:
            raise ValueError("bbox filters are supported only for vsats and theoretical_subscribers")
        where_clauses.extend(
            (
                "e.lat >= %s",
                "e.lat <= %s",
                "e.lon >= %s",
                "e.lon <= %s",
            )
        )
        params.extend((bbox["lat_min"], bbox["lat_max"], bbox["lon_min"], bbox["lon_max"]))

    if normalized_kind == "vsats":
        if region_codes:
            where_clauses.append("e.region_code = ANY(%s::integer[])")
            params.append(list(region_codes))
        if any(value is not None and value not in ([], {}) for value in (subject_codes, federal_districts, grid_cell_ids, is_active)):
            raise ValueError(
                "subject_codes, federal_districts, grid_cell_ids and is_active filters are supported only for theoretical_subscribers"
            )
    elif normalized_kind == "theoretical_subscribers":
        if subject_codes:
            where_clauses.append("e.subject_code = ANY(%s::integer[])")
            params.append(list(subject_codes))
        if federal_districts:
            where_clauses.append("e.federal_district = ANY(%s::text[])")
            params.append(list(federal_districts))
        if grid_cell_ids:
            where_clauses.append("e.grid_cell_id = ANY(%s::text[])")
            params.append(list(grid_cell_ids))
        if is_active is not None:
            where_clauses.append("e.is_active = %s")
            params.append(bool(is_active))
        if region_codes:
            raise ValueError("region_codes filters are supported only for vsats")
    elif any(value is not None and value not in ([], {}) for value in (subject_codes, federal_districts, grid_cell_ids, is_active, region_codes)):
        raise ValueError(
            "region_codes filters are supported only for vsats; subject_codes, federal_districts, grid_cell_ids and is_active filters are supported only for theoretical_subscribers"
        )

    where_sql = "" if not where_clauses else "WHERE " + " AND ".join(where_clauses)
    return where_sql, params


def _inventory_entity_from_row(normalized_kind: str, row: dict[str, Any]) -> InventoryEntityRecord:
    return InventoryEntityRecord(
        kind=normalized_kind,
        entity_key=str(row["entity_key"]),
        name=str(row["name"]),
        lat=None if row["lat"] is None else float(row["lat"]),
        lon=None if row["lon"] is None else float(row["lon"]),
        alt_m=None if row["alt_m"] is None else float(row["alt_m"]),
        radio_profile=None if row["radio_profile"] is None else str(row["radio_profile"]),
        ground_terminal_profile_key=None
        if row.get("ground_terminal_profile_key") is None
        else str(row["ground_terminal_profile_key"]),
        user_beam_profile_key=None
        if row.get("user_beam_profile_key") is None
        else str(row["user_beam_profile_key"]),
        feeder_beam_profile_key=None
        if row.get("feeder_beam_profile_key") is None
        else str(row["feeder_beam_profile_key"]),
        role=None if row["role"] is None else str(row["role"]),
        region_code=None if row["region_code"] is None else int(row["region_code"]),
        frequency=None if row["frequency"] is None else int(row["frequency"]),
        priority=None if row["priority"] is None else float(row["priority"]),
        subject_code=None if row.get("subject_code") is None else int(row["subject_code"]),
        subject_name=None if row.get("subject_name") is None else str(row["subject_name"]),
        federal_district=None if row.get("federal_district") is None else str(row["federal_district"]),
        grid_cell_id=None if row.get("grid_cell_id") is None else str(row["grid_cell_id"]),
        seed_version=None if row.get("seed_version") is None else str(row["seed_version"]),
        is_active=None if row.get("is_active") is None else bool(row["is_active"]),
        catalog_ordinal=None if row["catalog_ordinal"] is None else int(row["catalog_ordinal"]),
        metadata=dict(row["metadata"]) if isinstance(row["metadata"], dict) else {},
    )


def _ground_profile_from_row(row: dict[str, Any]) -> GroundTerminalProfileRecord:
    return GroundTerminalProfileRecord(
        profile_key=str(row["profile_key"]),
        station_kind=str(row["station_kind"]),
        params=dict(row["params"]) if isinstance(row.get("params"), dict) else {},
        metadata=dict(row["metadata"]) if isinstance(row.get("metadata"), dict) else {},
    )


def _relay_profile_from_row(row: dict[str, Any]) -> RelayPayloadProfileRecord:
    return RelayPayloadProfileRecord(
        profile_key=str(row["profile_key"]),
        relay_mode=str(row["relay_mode"]),
        params=dict(row["params"]) if isinstance(row.get("params"), dict) else {},
        metadata=dict(row["metadata"]) if isinstance(row.get("metadata"), dict) else {},
    )


def _profile_rule_from_row(row: dict[str, Any]) -> InventoryProfileAssignmentRuleRecord:
    bbox_raw = dict(row["bbox"]) if isinstance(row.get("bbox"), dict) else {}
    bbox = None if not bbox_raw else {str(key): float(value) for key, value in bbox_raw.items()}
    region_codes_raw = row.get("region_codes")
    subject_codes_raw = row.get("subject_codes")
    federal_districts_raw = row.get("federal_districts")
    grid_cell_ids_raw = row.get("grid_cell_ids")
    return InventoryProfileAssignmentRuleRecord(
        rule_id=int(row["rule_id"]),
        rule_key=str(row["rule_key"]),
        name=str(row["name"]),
        kind=str(row["kind"]),
        catalog_key=None if row.get("catalog_key") is None else str(row["catalog_key"]),
        query=None if row.get("query") is None else str(row["query"]),
        bbox=bbox,
        region_codes=tuple(int(value) for value in region_codes_raw) if isinstance(region_codes_raw, list) else (),
        subject_codes=tuple(int(value) for value in subject_codes_raw) if isinstance(subject_codes_raw, list) else (),
        federal_districts=tuple(str(value) for value in federal_districts_raw)
        if isinstance(federal_districts_raw, list)
        else (),
        grid_cell_ids=tuple(str(value) for value in grid_cell_ids_raw) if isinstance(grid_cell_ids_raw, list) else (),
        is_active_filter=None if row.get("is_active_filter") is None else bool(row["is_active_filter"]),
        ground_terminal_profile_key=None
        if row.get("ground_terminal_profile_key") is None
        else str(row["ground_terminal_profile_key"]),
        user_beam_profile_key=None if row.get("user_beam_profile_key") is None else str(row["user_beam_profile_key"]),
        feeder_beam_profile_key=None
        if row.get("feeder_beam_profile_key") is None
        else str(row["feeder_beam_profile_key"]),
        priority=int(row.get("priority", 100)),
        is_active=bool(row.get("is_active", True)),
        metadata=dict(row["metadata"]) if isinstance(row.get("metadata"), dict) else {},
    )


def _validate_profile_rule_kind(kind: str) -> str:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _KIND_ENTITY_CONFIG:
        raise ValueError(f"Unsupported inventory kind: {normalized_kind}")
    return normalized_kind


def _validate_profile_rule_targets(
    cursor: Any,
    *,
    kind: str,
    ground_terminal_profile_key: str | None,
    user_beam_profile_key: str | None,
    feeder_beam_profile_key: str | None,
) -> tuple[str | None, str | None, str | None]:
    ground_kinds = {"gateways", "targets", "vsats", "theoretical_subscribers"}
    relay_kinds = {"satellites", "haps"}
    normalized_ground = None if ground_terminal_profile_key is None else str(ground_terminal_profile_key).strip()
    normalized_user = None if user_beam_profile_key is None else str(user_beam_profile_key).strip()
    normalized_feeder = None if feeder_beam_profile_key is None else str(feeder_beam_profile_key).strip()

    if kind in ground_kinds:
        if not normalized_ground:
            raise ValueError("ground_terminal_profile_key is required for this inventory kind")
        if normalized_user or normalized_feeder:
            raise ValueError("relay profile keys are not supported for this inventory kind")
        rows = _fetch_rows(
            cursor,
            f"""
            SELECT profile_key
            FROM {INVENTORY_V2_SCHEMA}.ground_terminal_profile
            WHERE profile_key = %s
            """,
            (normalized_ground,),
        )
        if not rows:
            raise ValueError(f"Unknown ground terminal profile: {normalized_ground}")
        return normalized_ground, None, None

    if kind in relay_kinds:
        if normalized_ground:
            raise ValueError("ground terminal profile key is not supported for relay inventory kinds")
        requested = [value for value in (normalized_user, normalized_feeder) if value]
        if not requested:
            raise ValueError("user_beam_profile_key or feeder_beam_profile_key is required for relay kinds")
        rows = _fetch_rows(
            cursor,
            f"""
            SELECT profile_key
            FROM {INVENTORY_V2_SCHEMA}.relay_payload_profile
            WHERE profile_key = ANY(%s::text[])
            """,
            (requested,),
        )
        resolved = {str(row["profile_key"]) for row in rows}
        missing = [value for value in requested if value not in resolved]
        if missing:
            raise ValueError(f"Unknown relay payload profiles: {missing}")
        return None, normalized_user, normalized_feeder

    raise ValueError(f"Unsupported inventory kind: {kind}")


def _rule_resolved_keys(
    rule: InventoryProfileAssignmentRuleRecord,
    *,
    postgres: PostgresSettings | None = None,
) -> tuple[str, ...]:
    return resolve_inventory_entity_keys(
        rule.kind,
        query=rule.query,
        catalog_key=rule.catalog_key,
        bbox=rule.bbox,
        region_codes=list(rule.region_codes),
        subject_codes=list(rule.subject_codes),
        federal_districts=list(rule.federal_districts),
        grid_cell_ids=list(rule.grid_cell_ids),
        is_active=rule.is_active_filter,
        postgres=postgres,
    )


def list_ground_terminal_profiles(
    *,
    station_kind: str | None = None,
    postgres: PostgresSettings | None = None,
) -> tuple[GroundTerminalProfileRecord, ...]:
    normalized_kind = None if station_kind is None else str(station_kind).strip()
    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT profile_key, station_kind, params, metadata
                FROM {INVENTORY_V2_SCHEMA}.ground_terminal_profile
                WHERE (%s::text IS NULL OR station_kind = %s::text)
                ORDER BY station_kind, profile_key
                """,
                (normalized_kind, normalized_kind),
            )
    return tuple(_ground_profile_from_row(row) for row in rows)


def list_relay_payload_profiles(
    *,
    relay_mode: str | None = None,
    postgres: PostgresSettings | None = None,
) -> tuple[RelayPayloadProfileRecord, ...]:
    normalized_mode = None if relay_mode is None else str(relay_mode).strip()
    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT profile_key, relay_mode, params, metadata
                FROM {INVENTORY_V2_SCHEMA}.relay_payload_profile
                WHERE (%s::text IS NULL OR relay_mode = %s::text)
                ORDER BY relay_mode, profile_key
                """,
                (normalized_mode, normalized_mode),
            )
    return tuple(_relay_profile_from_row(row) for row in rows)


def upsert_ground_terminal_profile(
    *,
    profile_key: str,
    station_kind: str,
    params: dict[str, Any],
    metadata: dict[str, Any] | None = None,
    postgres: PostgresSettings | None = None,
) -> GroundTerminalProfileRecord:
    normalized_key = str(profile_key).strip()
    normalized_kind = str(station_kind).strip()
    if not normalized_key:
        raise ValueError("ground terminal profile_key must not be empty")
    if not normalized_kind:
        raise ValueError("ground terminal station_kind must not be empty")

    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()
    ensure_schema_v2(postgres)

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {INVENTORY_V2_SCHEMA}.ground_terminal_profile (
                    profile_key,
                    station_kind,
                    params,
                    metadata
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (profile_key) DO UPDATE SET
                    station_kind = EXCLUDED.station_kind,
                    params = EXCLUDED.params,
                    metadata = EXCLUDED.metadata
                """,
                (
                    normalized_key,
                    normalized_kind,
                    Jsonb(dict(params)),
                    Jsonb(dict(metadata or {})),
                ),
            )
        connection.commit()

    return GroundTerminalProfileRecord(
        profile_key=normalized_key,
        station_kind=normalized_kind,
        params=dict(params),
        metadata=dict(metadata or {}),
    )


def upsert_relay_payload_profile(
    *,
    profile_key: str,
    relay_mode: str,
    params: dict[str, Any],
    metadata: dict[str, Any] | None = None,
    postgres: PostgresSettings | None = None,
) -> RelayPayloadProfileRecord:
    normalized_key = str(profile_key).strip()
    normalized_mode = str(relay_mode).strip()
    if not normalized_key:
        raise ValueError("relay payload profile_key must not be empty")
    if not normalized_mode:
        raise ValueError("relay payload relay_mode must not be empty")

    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()
    ensure_schema_v2(postgres)

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {INVENTORY_V2_SCHEMA}.relay_payload_profile (
                    profile_key,
                    relay_mode,
                    params,
                    metadata
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (profile_key) DO UPDATE SET
                    relay_mode = EXCLUDED.relay_mode,
                    params = EXCLUDED.params,
                    metadata = EXCLUDED.metadata
                """,
                (
                    normalized_key,
                    normalized_mode,
                    Jsonb(dict(params)),
                    Jsonb(dict(metadata or {})),
                ),
            )
        connection.commit()

    return RelayPayloadProfileRecord(
        profile_key=normalized_key,
        relay_mode=normalized_mode,
        params=dict(params),
        metadata=dict(metadata or {}),
    )


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _stable_fingerprint(kind: str, key: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"kind": str(kind), "key": str(key), "payload": payload},
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


_ENTITY_WRITE_CONFIG = {
    "gateways": {
        "table": "gateway",
        "key_column": "gateway_key",
        "id_column": "gateway_id",
        "key_aliases": ("gateway_key", "entity_key", "key", "id"),
        "required": ("name", "lat", "lon"),
        "optional": (
            "site_alt_m",
            "antenna_height_agl_m",
            "radio_profile",
            "ground_terminal_profile_key",
            "role",
        ),
        "defaults": {"role": "gateway"},
        "float_fields": ("lat", "lon", "site_alt_m", "antenna_height_agl_m"),
        "int_fields": (),
        "bool_fields": (),
    },
    "targets": {
        "table": "target",
        "key_column": "target_key",
        "id_column": "target_id",
        "key_aliases": ("target_key", "entity_key", "key", "id"),
        "required": ("name", "lat", "lon", "frequency"),
        "optional": (
            "priority",
            "site_alt_m",
            "antenna_height_agl_m",
            "ground_terminal_profile_key",
        ),
        "defaults": {"priority": 0.0},
        "float_fields": ("lat", "lon", "priority", "site_alt_m", "antenna_height_agl_m"),
        "int_fields": ("frequency",),
        "bool_fields": (),
    },
    "satellites": {
        "table": "satellite",
        "key_column": "satellite_key",
        "id_column": "satellite_id",
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
    },
    "haps": {
        "table": "haps",
        "key_column": "haps_key",
        "id_column": "haps_id",
        "key_aliases": ("haps_key", "entity_key", "key", "id"),
        "required": ("name", "lat", "lon", "alt_m"),
        "optional": (
            "radio_profile",
            "user_beam_profile_key",
            "feeder_beam_profile_key",
            "connection_min",
        ),
        "defaults": {"connection_min": 0},
        "float_fields": ("lat", "lon", "alt_m"),
        "int_fields": ("connection_min",),
        "bool_fields": (),
    },
    "vsats": {
        "table": "vsat",
        "key_column": "vsat_key",
        "id_column": "vsat_id",
        "key_aliases": ("vsat_key", "entity_key", "key", "id"),
        "required": ("name", "lat", "lon", "region_code"),
        "optional": (
            "site_alt_m",
            "antenna_height_agl_m",
            "radio_profile",
            "ground_terminal_profile_key",
            "role",
        ),
        "defaults": {"role": "vsat"},
        "float_fields": ("lat", "lon", "site_alt_m", "antenna_height_agl_m"),
        "int_fields": ("region_code",),
        "bool_fields": (),
    },
    "theoretical_subscribers": {
        "table": "theoretical_subscriber",
        "key_column": "subscriber_key",
        "id_column": "theoretical_subscriber_id",
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
        ),
        "defaults": {"is_active": True},
        "float_fields": ("lat", "lon", "site_alt_m"),
        "int_fields": ("subject_code",),
        "bool_fields": ("is_active",),
    },
}

_ENTITY_WRITE_CONTROL_FIELDS = {
    "kind",
    "entity_key",
    "key",
    "id",
    "catalog_key",
    "catalog_name",
    "catalog_description",
    "catalog_ordinal",
    "source_type",
    "metadata",
}


def build_inventory_entity_write_draft(
    kind: str,
    payload: Mapping[str, Any],
    *,
    entity_key: str | None = None,
    catalog_key: str | None = None,
) -> InventoryEntityWriteDraft:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _ENTITY_WRITE_CONFIG:
        raise ValueError(
            "entity write API supports gateways, targets, satellites, haps, vsats "
            "and theoretical_subscribers at this stage"
        )
    if not isinstance(payload, Mapping):
        raise ValueError("entity payload must be an object")

    config = _ENTITY_WRITE_CONFIG[normalized_kind]
    key = _optional_entity_text(entity_key)
    if key is None:
        for alias in config["key_aliases"]:
            key = _optional_entity_text(payload.get(alias))
            if key is not None:
                break
    if key is None:
        raise ValueError(f"{config['key_column']} or entity_key is required")

    allowed_fields = (
        set(_ENTITY_WRITE_CONTROL_FIELDS)
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

    values: dict[str, Any] = {}
    for field_name, default_value in config["defaults"].items():
        values[field_name] = default_value
    for field_name in (*config["required"], *config["optional"]):
        if field_name not in payload:
            continue
        values[field_name] = payload[field_name]
    for field_name in config["required"]:
        if field_name not in values or _optional_entity_text(values.get(field_name)) is None:
            raise ValueError(f"{field_name} is required")
    for field_name in config["float_fields"]:
        if field_name in values and values[field_name] is not None:
            values[field_name] = float(values[field_name])
    for field_name in config["int_fields"]:
        if field_name in values and values[field_name] is not None:
            values[field_name] = int(values[field_name])
    for field_name in config.get("bool_fields", ()):
        if field_name in values and values[field_name] is not None:
            values[field_name] = _entity_bool(values[field_name])
    for field_name in (
        "name",
        "radio_profile",
        "ground_terminal_profile_key",
        "user_beam_profile_key",
        "feeder_beam_profile_key",
        "role",
        "tle_line1",
        "tle_line2",
        "beam_layout_mode",
        "subject_name",
        "federal_district",
        "grid_cell_id",
        "seed_version",
    ):
        if field_name in values:
            values[field_name] = _optional_entity_text(values[field_name])
    if not values.get("name"):
        raise ValueError("name is required")

    metadata = payload.get("metadata")
    if metadata is not None and not isinstance(metadata, Mapping):
        raise ValueError("metadata must be an object")

    return InventoryEntityWriteDraft(
        kind=normalized_kind,
        entity_key=key,
        name=str(values["name"]),
        values=values,
        catalog_key=_optional_entity_text(catalog_key)
        or _optional_entity_text(payload.get("catalog_key"))
        or "gui",
        catalog_name=_optional_entity_text(payload.get("catalog_name")),
        catalog_description=_optional_entity_text(payload.get("catalog_description")),
        catalog_ordinal=int(payload.get("catalog_ordinal", 0)),
        source_type=_optional_entity_text(payload.get("source_type")) or "api",
        metadata=dict(metadata or {}),
    )


def upsert_inventory_entity(
    kind: str,
    payload: Mapping[str, Any],
    *,
    entity_key: str | None = None,
    catalog_key: str | None = None,
    postgres: PostgresSettings | None = None,
) -> InventoryEntityRecord:
    draft = build_inventory_entity_write_draft(
        kind,
        payload,
        entity_key=entity_key,
        catalog_key=catalog_key,
    )
    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()
    ensure_schema_v2(postgres)
    config = _ENTITY_WRITE_CONFIG[draft.kind]
    fingerprint_payload = {key: value for key, value in sorted(draft.values.items())}
    fingerprint = _stable_fingerprint(draft.kind, draft.entity_key, fingerprint_payload)

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            entity_id = _upsert_entity_row(cursor, Jsonb, draft, config, fingerprint)
            catalog_id = _ensure_entity_catalog(cursor, Jsonb, draft)
            member_config = _KIND_ENTITY_CONFIG[draft.kind]
            cursor.execute(
                f"""
                INSERT INTO {INVENTORY_V2_SCHEMA}.{member_config["member_table"]} (
                    catalog_id, {member_config["member_entity_column"]}, ordinal, metadata
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (catalog_id, {member_config["member_entity_column"]}) DO UPDATE SET
                    ordinal = EXCLUDED.ordinal,
                    metadata = EXCLUDED.metadata
                """,
                (
                    catalog_id,
                    entity_id,
                    draft.catalog_ordinal,
                    Jsonb({"source": draft.source_type}),
                ),
            )
        connection.commit()
    return draft.to_record()


def update_inventory_entity_resource_limits(
    kind: str,
    entity_key: str,
    resource_limits: Mapping[str, Any],
    *,
    postgres: PostgresSettings | None = None,
) -> None:
    normalized_kind = str(kind).strip()
    if normalized_kind not in {"satellites", "haps"}:
        raise ValueError("resource limits mirror supports only satellites and haps")
    config = _KIND_ENTITY_CONFIG[normalized_kind]
    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()
    ensure_schema_v2(postgres)
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE {INVENTORY_V2_SCHEMA}.{config["table"]}
                SET resource_limits = %s
                WHERE {config["key_column"]} = %s
                """,
                (Jsonb(dict(resource_limits)), str(entity_key)),
            )
        connection.commit()


def update_inventory_entity_allowed_link_types(
    kind: str,
    entity_key: str,
    link_types: tuple[str, ...] | list[str],
    *,
    postgres: PostgresSettings | None = None,
) -> None:
    normalized_kind = str(kind).strip()
    if normalized_kind != "satellites":
        return
    config = _KIND_ENTITY_CONFIG[normalized_kind]
    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()
    ensure_schema_v2(postgres)
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE {INVENTORY_V2_SCHEMA}.{config["table"]}
                SET allowed_link_types = %s
                WHERE {config["key_column"]} = %s
                """,
                (Jsonb([str(item) for item in link_types]), str(entity_key)),
            )
        connection.commit()


def _upsert_entity_row(
    cursor: Any,
    Jsonb: Any,
    draft: InventoryEntityWriteDraft,
    config: Mapping[str, Any],
    fingerprint: str,
) -> int:
    columns = [str(config["key_column"]), "fingerprint_sha1", *config["required"], *config["optional"], "metadata"]
    values = [
        draft.entity_key,
        fingerprint,
        *(draft.values.get(field_name) for field_name in config["required"]),
        *(draft.values.get(field_name) for field_name in config["optional"]),
        Jsonb(dict(draft.metadata)),
    ]
    placeholders = ", ".join(["%s"] * len(columns))
    update_columns = [column for column in columns if column != str(config["key_column"])]
    rows = _fetch_rows(
        cursor,
        f"""
        INSERT INTO {INVENTORY_V2_SCHEMA}.{config["table"]} (
            {", ".join(columns)}
        )
        VALUES ({placeholders})
        ON CONFLICT ({config["key_column"]}) DO UPDATE SET
            {", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)}
        RETURNING {config["id_column"]} AS entity_id
        """,
        tuple(values),
    )
    return int(rows[0]["entity_id"])


def _ensure_entity_catalog(
    cursor: Any,
    Jsonb: Any,
    draft: InventoryEntityWriteDraft,
) -> int:
    rows = _fetch_rows(
        cursor,
        f"""
        INSERT INTO {INVENTORY_V2_SCHEMA}.selection_catalog (
            kind, catalog_key, name, description, source_type, metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (kind, catalog_key) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, {INVENTORY_V2_SCHEMA}.selection_catalog.name),
            description = COALESCE(EXCLUDED.description, {INVENTORY_V2_SCHEMA}.selection_catalog.description),
            source_type = EXCLUDED.source_type
        RETURNING catalog_id
        """,
        (
            draft.kind,
            draft.catalog_key,
            draft.catalog_name,
            draft.catalog_description,
            draft.source_type,
            Jsonb({}),
        ),
    )
    return int(rows[0]["catalog_id"])


def _optional_entity_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return None if not text else text


def _entity_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
        raise ValueError("boolean entity field has an invalid value")
    return bool(value)


def list_frequency_plans(
    *,
    postgres: PostgresSettings | None = None,
) -> tuple[FrequencyPlanRecord, ...]:
    postgres = postgres or load_postgres_settings()
    ensure_schema_v2(postgres)
    psycopg = _require_psycopg()
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT p.plan_key, p.name, p.description, p.metadata,
                       COUNT(c.channel_index) AS channel_count
                FROM {INVENTORY_V2_SCHEMA}.frequency_plan p
                LEFT JOIN {INVENTORY_V2_SCHEMA}.frequency_channel c
                  ON c.frequency_plan_id = p.frequency_plan_id
                GROUP BY p.frequency_plan_id, p.plan_key, p.name, p.description, p.metadata
                ORDER BY p.plan_key
                """,
                (),
            )
    return tuple(
        FrequencyPlanRecord(
            plan_key=str(row["plan_key"]),
            name=str(row["name"]),
            description=None if row.get("description") is None else str(row["description"]),
            metadata={**_metadata_dict(row.get("metadata")), "channel_count": int(row.get("channel_count") or 0)},
            channels=(),
        )
        for row in rows
    )


def load_frequency_plan(
    plan_key: str,
    *,
    postgres: PostgresSettings | None = None,
) -> FrequencyPlanRecord | None:
    normalized_key = str(plan_key).strip()
    if not normalized_key:
        raise ValueError("frequency plan_key must not be empty")
    postgres = postgres or load_postgres_settings()
    ensure_schema_v2(postgres)
    psycopg = _require_psycopg()
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            plans = _fetch_rows(
                cursor,
                f"""
                SELECT frequency_plan_id, plan_key, name, description, metadata
                FROM {INVENTORY_V2_SCHEMA}.frequency_plan
                WHERE plan_key = %s
                """,
                (normalized_key,),
            )
            if not plans:
                return None
            plan = plans[0]
            channels = _fetch_rows(
                cursor,
                f"""
                SELECT channel_index, center_frequency_ghz, bandwidth_mhz, metadata
                FROM {INVENTORY_V2_SCHEMA}.frequency_channel
                WHERE frequency_plan_id = %s
                ORDER BY channel_index
                """,
                (int(plan["frequency_plan_id"]),),
            )
    return FrequencyPlanRecord(
        plan_key=str(plan["plan_key"]),
        name=str(plan["name"]),
        description=None if plan.get("description") is None else str(plan["description"]),
        metadata=_metadata_dict(plan.get("metadata")),
        channels=tuple(
            FrequencyChannelRecord(
                channel_index=int(row["channel_index"]),
                center_frequency_ghz=float(row["center_frequency_ghz"]),
                bandwidth_mhz=float(row["bandwidth_mhz"]),
                metadata=_metadata_dict(row.get("metadata")),
            )
            for row in channels
        ),
    )


def upsert_frequency_plan(
    *,
    plan_key: str,
    name: str | None = None,
    description: str | None = None,
    channels: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    metadata: dict[str, Any] | None = None,
    postgres: PostgresSettings | None = None,
) -> FrequencyPlanRecord:
    normalized_key = str(plan_key).strip()
    if not normalized_key:
        raise ValueError("frequency plan_key must not be empty")
    normalized_channels = [
        FrequencyChannelRecord(
            channel_index=int(item["channel_index"]),
            center_frequency_ghz=float(item["center_frequency_ghz"]),
            bandwidth_mhz=float(item["bandwidth_mhz"]),
            metadata=_metadata_dict(item.get("metadata")),
        )
        for item in channels
        if isinstance(item, dict)
    ]
    if not normalized_channels:
        raise ValueError("frequency plan must include at least one channel")

    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()
    ensure_schema_v2(postgres)
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {INVENTORY_V2_SCHEMA}.frequency_plan (
                    plan_key, name, description, metadata, updated_at
                ) VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (plan_key) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    metadata = EXCLUDED.metadata,
                    updated_at = now()
                RETURNING frequency_plan_id
                """,
                (
                    normalized_key,
                    str(name or normalized_key),
                    None if description is None else str(description),
                    Jsonb(dict(metadata or {})),
                ),
            )
            plan_id = int(cursor.fetchone()[0])
            cursor.execute(
                f"DELETE FROM {INVENTORY_V2_SCHEMA}.frequency_channel WHERE frequency_plan_id = %s",
                (plan_id,),
            )
            cursor.executemany(
                f"""
                INSERT INTO {INVENTORY_V2_SCHEMA}.frequency_channel (
                    frequency_plan_id, channel_index, center_frequency_ghz, bandwidth_mhz, metadata
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                [
                    (
                        plan_id,
                        channel.channel_index,
                        channel.center_frequency_ghz,
                        channel.bandwidth_mhz,
                        Jsonb(dict(channel.metadata)),
                    )
                    for channel in normalized_channels
                ],
            )
        connection.commit()

    return FrequencyPlanRecord(
        plan_key=normalized_key,
        name=str(name or normalized_key),
        description=None if description is None else str(description),
        metadata=dict(metadata or {}),
        channels=tuple(sorted(normalized_channels, key=lambda item: int(item.channel_index))),
    )


def preview_satellite_constellation(spec: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    return tuple(item.to_dict() for item in constellation.generate_constellation_satellites(spec))


def save_satellite_constellation(
    spec: dict[str, Any],
    *,
    persist_satellites: bool = True,
    postgres: PostgresSettings | None = None,
) -> SatelliteConstellationRecord:
    constellation_key = str(spec.get("constellation_key") or spec.get("key") or "").strip()
    if not constellation_key:
        raise ValueError("constellation_key must not be empty")
    satellites = constellation.generate_constellation_satellites(spec)
    params = dict(spec)
    metadata = _metadata_dict(params.pop("metadata", {}))
    name = str(params.get("name") or constellation_key)

    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()
    ensure_schema_v2(postgres)
    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {INVENTORY_V2_SCHEMA}.satellite_constellation (
                    constellation_key, name, params, metadata, updated_at
                ) VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (constellation_key) DO UPDATE SET
                    name = EXCLUDED.name,
                    params = EXCLUDED.params,
                    metadata = EXCLUDED.metadata,
                    updated_at = now()
                """,
                (constellation_key, name, Jsonb(params), Jsonb(metadata)),
            )
            if persist_satellites:
                cursor.execute(
                    f"""
                    INSERT INTO {INVENTORY_V2_SCHEMA}.selection_catalog (
                        kind, catalog_key, name, description, source_type, metadata
                    ) VALUES ('satellites', %s, %s, %s, 'generated_constellation', %s)
                    ON CONFLICT (kind, catalog_key) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        source_type = EXCLUDED.source_type,
                        metadata = EXCLUDED.metadata
                    RETURNING catalog_id
                    """,
                    (
                        constellation_key,
                        name,
                        f"Generated constellation {constellation_key}",
                        Jsonb({"constellation_key": constellation_key}),
                    ),
                )
                catalog_id = int(cursor.fetchone()[0])
                cursor.execute(
                    f"DELETE FROM {INVENTORY_V2_SCHEMA}.satellite_catalog_member WHERE catalog_id = %s",
                    (catalog_id,),
                )
                for ordinal, generated in enumerate(satellites):
                    payload = generated.to_scenario_dict()
                    fingerprint = _stable_fingerprint("satellites", generated.external_id, payload)
                    satellite_metadata = {
                        **generated.metadata,
                        "constellation_key": constellation_key,
                    }
                    cursor.execute(
                        f"""
                        INSERT INTO {INVENTORY_V2_SCHEMA}.satellite (
                            satellite_key,
                            fingerprint_sha1,
                            name,
                            tle_line1,
                            tle_line2,
                            radio_profile,
                            user_beam_profile_key,
                            feeder_beam_profile_key,
                            connection_min,
                            beam_layout_mode,
                            dynamic_ray_count,
                            dynamic_ray_aperture_deg,
                            sat_haps_ray_count,
                            resource_limits,
                            allowed_link_types,
                            metadata
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (satellite_key) DO UPDATE SET
                            fingerprint_sha1 = EXCLUDED.fingerprint_sha1,
                            name = EXCLUDED.name,
                            tle_line1 = EXCLUDED.tle_line1,
                            tle_line2 = EXCLUDED.tle_line2,
                            radio_profile = EXCLUDED.radio_profile,
                            user_beam_profile_key = EXCLUDED.user_beam_profile_key,
                            feeder_beam_profile_key = EXCLUDED.feeder_beam_profile_key,
                            connection_min = EXCLUDED.connection_min,
                            beam_layout_mode = EXCLUDED.beam_layout_mode,
                            dynamic_ray_count = EXCLUDED.dynamic_ray_count,
                            dynamic_ray_aperture_deg = EXCLUDED.dynamic_ray_aperture_deg,
                            sat_haps_ray_count = EXCLUDED.sat_haps_ray_count,
                            resource_limits = EXCLUDED.resource_limits,
                            allowed_link_types = EXCLUDED.allowed_link_types,
                            metadata = EXCLUDED.metadata
                        RETURNING satellite_id
                        """,
                        (
                            generated.external_id,
                            fingerprint,
                            generated.name,
                            generated.tle_line1,
                            generated.tle_line2,
                            str(spec.get("radio_profile") or "default"),
                            str(spec.get("user_beam_profile_key") or spec.get("radio_profile") or "default"),
                            str(spec.get("feeder_beam_profile_key") or spec.get("radio_profile") or "default"),
                            int(spec.get("connection_min", 2)),
                            str(spec.get("beam_layout_mode") or "free"),
                            int(spec.get("dynamic_ray_count", 16)),
                            float(spec.get("dynamic_ray_aperture_deg", 1.5)),
                            int(spec.get("sat_haps_ray_count", 4)),
                            Jsonb(dict(spec.get("resource_limits", {})) if isinstance(spec.get("resource_limits"), dict) else {}),
                            Jsonb(list(spec.get("allowed_link_types", [])) if isinstance(spec.get("allowed_link_types"), list) else []),
                            Jsonb(satellite_metadata),
                        ),
                    )
                    satellite_id = int(cursor.fetchone()[0])
                    cursor.execute(
                        f"""
                        INSERT INTO {INVENTORY_V2_SCHEMA}.satellite_catalog_member (
                            catalog_id, satellite_id, ordinal, metadata
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (catalog_id, satellite_id) DO UPDATE SET
                            ordinal = EXCLUDED.ordinal,
                            metadata = EXCLUDED.metadata
                        """,
                        (
                            catalog_id,
                            satellite_id,
                            ordinal,
                            Jsonb({"constellation_key": constellation_key}),
                        ),
                    )
        connection.commit()

    return SatelliteConstellationRecord(
        constellation_key=constellation_key,
        name=name,
        params=params,
        metadata=metadata,
        satellite_count=len(satellites),
    )


def list_profile_assignment_rules(
    *,
    kind: str | None = None,
    is_active: bool | None = None,
    postgres: PostgresSettings | None = None,
) -> tuple[InventoryProfileAssignmentRuleRecord, ...]:
    normalized_kind = None if kind is None else _validate_profile_rule_kind(kind)
    postgres = postgres or load_postgres_settings()
    ensure_schema_v2(postgres)
    psycopg = _require_psycopg()

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT
                    rule_id,
                    rule_key,
                    name,
                    kind,
                    catalog_key,
                    query,
                    bbox,
                    region_codes,
                    subject_codes,
                    federal_districts,
                    grid_cell_ids,
                    is_active_filter,
                    ground_terminal_profile_key,
                    user_beam_profile_key,
                    feeder_beam_profile_key,
                    priority,
                    is_active,
                    metadata
                FROM {INVENTORY_V2_SCHEMA}.profile_assignment_rule
                WHERE (%s::text IS NULL OR kind = %s::text)
                  AND (%s::boolean IS NULL OR is_active = %s::boolean)
                ORDER BY priority, rule_id
                """,
                (normalized_kind, normalized_kind, is_active, is_active),
            )
    return tuple(_profile_rule_from_row(row) for row in rows)


def load_profile_assignment_rule(
    rule_id: int,
    *,
    postgres: PostgresSettings | None = None,
) -> InventoryProfileAssignmentRuleRecord:
    postgres = postgres or load_postgres_settings()
    ensure_schema_v2(postgres)
    psycopg = _require_psycopg()

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT
                    rule_id,
                    rule_key,
                    name,
                    kind,
                    catalog_key,
                    query,
                    bbox,
                    region_codes,
                    subject_codes,
                    federal_districts,
                    grid_cell_ids,
                    is_active_filter,
                    ground_terminal_profile_key,
                    user_beam_profile_key,
                    feeder_beam_profile_key,
                    priority,
                    is_active,
                    metadata
                FROM {INVENTORY_V2_SCHEMA}.profile_assignment_rule
                WHERE rule_id = %s
                """,
                (int(rule_id),),
            )
            if not rows:
                raise ValueError(f"Profile assignment rule {rule_id} was not found")
    return _profile_rule_from_row(rows[0])


def save_profile_assignment_rule(
    *,
    name: str,
    kind: str,
    rule_id: int | None = None,
    rule_key: str | None = None,
    catalog_key: str | None = None,
    query: str | None = None,
    bbox: dict[str, float] | None = None,
    region_codes: list[int] | tuple[int, ...] | None = None,
    subject_codes: list[int] | tuple[int, ...] | None = None,
    federal_districts: list[str] | tuple[str, ...] | None = None,
    grid_cell_ids: list[str] | tuple[str, ...] | None = None,
    is_active_filter: bool | None = None,
    ground_terminal_profile_key: str | None = None,
    user_beam_profile_key: str | None = None,
    feeder_beam_profile_key: str | None = None,
    priority: int = 100,
    is_active: bool = True,
    metadata: dict[str, Any] | None = None,
    postgres: PostgresSettings | None = None,
) -> InventoryProfileAssignmentRuleRecord:
    normalized_name = str(name).strip()
    if not normalized_name:
        raise ValueError("profile assignment rule name must not be empty")
    normalized_kind = _validate_profile_rule_kind(kind)
    normalized_rule_key = str(rule_key or f"profile-rule-{uuid4().hex}").strip()
    if not normalized_rule_key:
        raise ValueError("profile assignment rule key must not be empty")
    normalized_catalog_key = None if catalog_key is None else str(catalog_key).strip() or None
    normalized_query = None if query is None else str(query).strip() or None
    normalized_bbox = _normalize_bbox(bbox)
    normalized_region_codes = tuple(int(value) for value in region_codes) if region_codes else ()
    normalized_subject_codes = tuple(int(value) for value in subject_codes) if subject_codes else ()
    normalized_districts = tuple(_normalize_text_list(federal_districts)) if federal_districts else ()
    normalized_grid_ids = tuple(_normalize_text_list(grid_cell_ids)) if grid_cell_ids else ()

    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()
    ensure_schema_v2(postgres)

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            normalized_ground, normalized_user, normalized_feeder = _validate_profile_rule_targets(
                cursor,
                kind=normalized_kind,
                ground_terminal_profile_key=ground_terminal_profile_key,
                user_beam_profile_key=user_beam_profile_key,
                feeder_beam_profile_key=feeder_beam_profile_key,
            )
            if rule_id is None:
                cursor.execute(
                    f"""
                    INSERT INTO {INVENTORY_V2_SCHEMA}.profile_assignment_rule (
                        rule_key,
                        name,
                        kind,
                        catalog_key,
                        query,
                        bbox,
                        region_codes,
                        subject_codes,
                        federal_districts,
                        grid_cell_ids,
                        is_active_filter,
                        ground_terminal_profile_key,
                        user_beam_profile_key,
                        feeder_beam_profile_key,
                        priority,
                        is_active,
                        metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING rule_id
                    """,
                    (
                        normalized_rule_key,
                        normalized_name,
                        normalized_kind,
                        normalized_catalog_key,
                        normalized_query,
                        Jsonb(dict(normalized_bbox or {})),
                        Jsonb(list(normalized_region_codes)),
                        Jsonb(list(normalized_subject_codes)),
                        Jsonb(list(normalized_districts)),
                        Jsonb(list(normalized_grid_ids)),
                        is_active_filter,
                        normalized_ground,
                        normalized_user,
                        normalized_feeder,
                        int(priority),
                        bool(is_active),
                        Jsonb(dict(metadata or {})),
                    ),
                )
                resolved_rule_id = int(cursor.fetchone()[0])
            else:
                cursor.execute(
                    f"""
                    UPDATE {INVENTORY_V2_SCHEMA}.profile_assignment_rule
                    SET rule_key = %s,
                        name = %s,
                        kind = %s,
                        catalog_key = %s,
                        query = %s,
                        bbox = %s,
                        region_codes = %s,
                        subject_codes = %s,
                        federal_districts = %s,
                        grid_cell_ids = %s,
                        is_active_filter = %s,
                        ground_terminal_profile_key = %s,
                        user_beam_profile_key = %s,
                        feeder_beam_profile_key = %s,
                        priority = %s,
                        is_active = %s,
                        metadata = %s,
                        updated_at = now()
                    WHERE rule_id = %s
                    """,
                    (
                        normalized_rule_key,
                        normalized_name,
                        normalized_kind,
                        normalized_catalog_key,
                        normalized_query,
                        Jsonb(dict(normalized_bbox or {})),
                        Jsonb(list(normalized_region_codes)),
                        Jsonb(list(normalized_subject_codes)),
                        Jsonb(list(normalized_districts)),
                        Jsonb(list(normalized_grid_ids)),
                        is_active_filter,
                        normalized_ground,
                        normalized_user,
                        normalized_feeder,
                        int(priority),
                        bool(is_active),
                        Jsonb(dict(metadata or {})),
                        int(rule_id),
                    ),
                )
                if int(cursor.rowcount) != 1:
                    raise ValueError(f"Profile assignment rule {rule_id} was not found")
                resolved_rule_id = int(rule_id)
        connection.commit()

    return load_profile_assignment_rule(resolved_rule_id, postgres=postgres)


def preview_profile_assignment_rule(
    rule_id: int,
    *,
    preview_limit: int = 100,
    postgres: PostgresSettings | None = None,
) -> InventoryProfileAssignmentRulePreview:
    rule = load_profile_assignment_rule(int(rule_id), postgres=postgres)
    matched_keys = _rule_resolved_keys(rule, postgres=postgres)
    limit_value = max(1, int(preview_limit))
    return InventoryProfileAssignmentRulePreview(
        rule=rule,
        matched_count=len(matched_keys),
        entity_keys_preview=matched_keys[:limit_value],
        truncated=len(matched_keys) > limit_value,
    )


def apply_profile_assignment_rule(
    rule_id: int,
    *,
    postgres: PostgresSettings | None = None,
) -> InventoryProfileRuleApplicationResult:
    rule = load_profile_assignment_rule(int(rule_id), postgres=postgres)
    matched_keys = _rule_resolved_keys(rule, postgres=postgres)
    if matched_keys:
        assignment = assign_inventory_profile_keys(
            rule.kind,
            entity_keys=matched_keys,
            ground_terminal_profile_key=rule.ground_terminal_profile_key,
            user_beam_profile_key=rule.user_beam_profile_key,
            feeder_beam_profile_key=rule.feeder_beam_profile_key,
            postgres=postgres,
        )
    else:
        assignment = InventoryProfileAssignmentResult(
            kind=rule.kind,
            updated_count=0,
            entity_keys=(),
            ground_terminal_profile_key=rule.ground_terminal_profile_key,
            user_beam_profile_key=rule.user_beam_profile_key,
            feeder_beam_profile_key=rule.feeder_beam_profile_key,
        )
    return InventoryProfileRuleApplicationResult(rule=rule, assignment=assignment)


def apply_active_profile_assignment_rules(
    *,
    kind: str | None = None,
    postgres: PostgresSettings | None = None,
) -> InventoryProfileRuleBatchResult:
    rules = list_profile_assignment_rules(kind=kind, is_active=True, postgres=postgres)
    results: list[InventoryProfileRuleApplicationResult] = []
    total_updated_count = 0
    for rule in rules:
        application = apply_profile_assignment_rule(rule.rule_id, postgres=postgres)
        results.append(application)
        total_updated_count += int(application.assignment.updated_count)
    return InventoryProfileRuleBatchResult(
        results=tuple(results),
        total_updated_count=total_updated_count,
    )


def resolve_inventory_entity_keys(
    kind: str,
    *,
    query: str | None = None,
    catalog_key: str | None = None,
    bbox: dict[str, float] | None = None,
    region_codes: list[int] | None = None,
    subject_codes: list[int] | None = None,
    federal_districts: list[str] | None = None,
    grid_cell_ids: list[str] | None = None,
    is_active: bool | None = None,
    postgres: PostgresSettings | None = None,
) -> tuple[str, ...]:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _KIND_ENTITY_CONFIG:
        raise ValueError(f"Unsupported inventory kind: {normalized_kind}")

    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()
    config = _KIND_ENTITY_CONFIG[normalized_kind]
    bbox_filter = _normalize_bbox(bbox)
    region_code_values = None if region_codes is None else [int(value) for value in region_codes]
    subject_code_values = None if subject_codes is None else [int(value) for value in subject_codes]
    district_values = None if federal_districts is None else _normalize_text_list(federal_districts)
    grid_values = None if grid_cell_ids is None else _normalize_text_list(grid_cell_ids)

    join_sql = ""
    params: list[Any] = []
    if catalog_key is not None:
        join_sql = f"""
            JOIN {INVENTORY_V2_SCHEMA}.selection_catalog c
              ON c.kind = %s AND c.catalog_key = %s
            JOIN {INVENTORY_V2_SCHEMA}.{config["member_table"]} m
              ON m.catalog_id = c.catalog_id
             AND m.{config["member_entity_column"]} = e.{config["entity_id_column"]}
        """
        params.extend((normalized_kind, str(catalog_key)))

    where_sql, where_params = _search_query_parts(
        normalized_kind,
        config,
        query=query,
        bbox=bbox_filter,
        region_codes=region_code_values,
        subject_codes=subject_code_values,
        federal_districts=district_values,
        grid_cell_ids=grid_values,
        is_active=is_active,
    )
    params.extend(where_params)
    order_sql = (
        f"ORDER BY m.ordinal, e.{config['key_column']}"
        if catalog_key is not None
        else f"ORDER BY e.{config['key_column']}"
    )

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT e.{config["key_column"]} AS entity_key
                FROM {INVENTORY_V2_SCHEMA}.{config["table"]} e
                {join_sql}
                {where_sql}
                {order_sql}
                """,
                tuple(params),
            )
    return tuple(str(row["entity_key"]) for row in rows)


def assign_inventory_profile_keys(
    kind: str,
    *,
    entity_keys: tuple[str, ...] | list[str],
    ground_terminal_profile_key: str | None = None,
    user_beam_profile_key: str | None = None,
    feeder_beam_profile_key: str | None = None,
    postgres: PostgresSettings | None = None,
) -> InventoryProfileAssignmentResult:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _KIND_ENTITY_CONFIG:
        raise ValueError(f"Unsupported inventory kind: {normalized_kind}")

    normalized_keys = tuple(dict.fromkeys(str(item).strip() for item in entity_keys if str(item).strip()))
    if not normalized_keys:
        raise ValueError("profile assignment requires at least one entity key")

    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()
    config = _KIND_ENTITY_CONFIG[normalized_kind]

    ground_kinds = {"gateways", "targets", "vsats", "theoretical_subscribers"}
    relay_kinds = {"satellites", "haps"}
    updates: list[str] = []
    params: list[Any] = []

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            existing_rows = _fetch_rows(
                cursor,
                f"""
                SELECT {config["key_column"]} AS entity_key
                FROM {INVENTORY_V2_SCHEMA}.{config["table"]}
                WHERE {config["key_column"]} = ANY(%s::text[])
                """,
                (list(normalized_keys),),
            )
            existing_keys = {str(row["entity_key"]) for row in existing_rows}
            missing_entity_keys = [value for value in normalized_keys if value not in existing_keys]
            if missing_entity_keys:
                raise ValueError(
                    f"Inventory profile assignment for '{normalized_kind}' contains unknown entity keys: {missing_entity_keys}"
                )

            if normalized_kind in ground_kinds:
                normalized_ground = None if ground_terminal_profile_key is None else str(ground_terminal_profile_key).strip()
                if not normalized_ground:
                    raise ValueError("ground_terminal_profile_key is required for this inventory kind")
                ground_rows = _fetch_rows(
                    cursor,
                    f"""
                    SELECT profile_key
                    FROM {INVENTORY_V2_SCHEMA}.ground_terminal_profile
                    WHERE profile_key = %s
                    """,
                    (normalized_ground,),
                )
                if not ground_rows:
                    raise ValueError(f"Unknown ground terminal profile: {normalized_ground}")
                updates.append("ground_terminal_profile_key = %s")
                params.append(normalized_ground)
            elif normalized_kind in relay_kinds:
                normalized_user = None if user_beam_profile_key is None else str(user_beam_profile_key).strip()
                normalized_feeder = None if feeder_beam_profile_key is None else str(feeder_beam_profile_key).strip()
                if not normalized_user and not normalized_feeder:
                    raise ValueError("user_beam_profile_key or feeder_beam_profile_key is required for relay kinds")
                requested = [value for value in (normalized_user, normalized_feeder) if value]
                if requested:
                    rows = _fetch_rows(
                        cursor,
                        f"""
                        SELECT profile_key
                        FROM {INVENTORY_V2_SCHEMA}.relay_payload_profile
                        WHERE profile_key = ANY(%s::text[])
                        """,
                        (requested,),
                    )
                    resolved = {str(row["profile_key"]) for row in rows}
                    missing = [value for value in requested if value not in resolved]
                    if missing:
                        raise ValueError(f"Unknown relay payload profiles: {missing}")
                if normalized_user:
                    updates.append("user_beam_profile_key = %s")
                    params.append(normalized_user)
                if normalized_feeder:
                    updates.append("feeder_beam_profile_key = %s")
                    params.append(normalized_feeder)
            else:
                raise ValueError(f"Unsupported inventory kind: {normalized_kind}")

            if not updates:
                raise ValueError("No profile assignment fields were provided")

            cursor.execute(
                f"""
                UPDATE {INVENTORY_V2_SCHEMA}.{config["table"]}
                SET {", ".join(updates)}
                WHERE {config["key_column"]} = ANY(%s::text[])
                """,
                tuple(params + [list(normalized_keys)]),
            )
            updated_count = int(cursor.rowcount)
        connection.commit()

    return InventoryProfileAssignmentResult(
        kind=normalized_kind,
        updated_count=updated_count,
        entity_keys=normalized_keys,
        ground_terminal_profile_key=None
        if ground_terminal_profile_key is None
        else str(ground_terminal_profile_key).strip(),
        user_beam_profile_key=None if user_beam_profile_key is None else str(user_beam_profile_key).strip(),
        feeder_beam_profile_key=None
        if feeder_beam_profile_key is None
        else str(feeder_beam_profile_key).strip(),
    )


def load_selection_profile(
    profile_id: int,
    postgres: PostgresSettings | None = None,
) -> SelectionProfileRecord:
    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT profile_id, profile_key, name, description, source_type, is_active, metadata
                FROM {INVENTORY_V2_SCHEMA}.selection_profile
                WHERE profile_id = %s
                """,
                (int(profile_id),),
            )
            if not rows:
                raise ValueError(f"Selection profile {profile_id} was not found")
            row = rows[0]
            return SelectionProfileRecord(
                profile_id=int(row["profile_id"]),
                profile_key=str(row["profile_key"]),
                name=str(row["name"]),
                description=None if row["description"] is None else str(row["description"]),
                source_type=str(row["source_type"]),
                is_active=bool(row["is_active"]),
                metadata=dict(row["metadata"]) if isinstance(row["metadata"], dict) else {},
                items=_profile_items(cursor, int(profile_id)),
            )


def list_selection_catalogs(
    *,
    kind: str | None = None,
    postgres: PostgresSettings | None = None,
) -> tuple[SelectionCatalogRecord, ...]:
    normalized_kind = None if kind is None else str(kind).strip()
    if normalized_kind is not None and normalized_kind not in _KIND_ENTITY_CONFIG:
        raise ValueError(f"Unsupported inventory kind for catalogs: {normalized_kind}")

    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT
                    c.catalog_id,
                    c.kind,
                    c.catalog_key,
                    c.name,
                    c.description,
                    c.source_type,
                    c.metadata,
                    CASE c.kind
                        WHEN 'gateways' THEN (SELECT COUNT(*) FROM {INVENTORY_V2_SCHEMA}.gateway_catalog_member m WHERE m.catalog_id = c.catalog_id)
                        WHEN 'targets' THEN (SELECT COUNT(*) FROM {INVENTORY_V2_SCHEMA}.target_catalog_member m WHERE m.catalog_id = c.catalog_id)
                        WHEN 'satellites' THEN (SELECT COUNT(*) FROM {INVENTORY_V2_SCHEMA}.satellite_catalog_member m WHERE m.catalog_id = c.catalog_id)
                        WHEN 'haps' THEN (SELECT COUNT(*) FROM {INVENTORY_V2_SCHEMA}.haps_catalog_member m WHERE m.catalog_id = c.catalog_id)
                        WHEN 'vsats' THEN (SELECT COUNT(*) FROM {INVENTORY_V2_SCHEMA}.vsat_catalog_member m WHERE m.catalog_id = c.catalog_id)
                        WHEN 'theoretical_subscribers' THEN (
                            SELECT COUNT(*)
                            FROM {INVENTORY_V2_SCHEMA}.theoretical_subscriber_catalog_member m
                            WHERE m.catalog_id = c.catalog_id
                        )
                        ELSE 0
                    END AS member_count
                FROM {INVENTORY_V2_SCHEMA}.selection_catalog c
                WHERE (%s::text IS NULL OR c.kind = %s::text)
                ORDER BY c.kind, c.catalog_key
                """,
                (normalized_kind, normalized_kind),
            )
    return tuple(
        SelectionCatalogRecord(
            catalog_id=int(row["catalog_id"]),
            kind=str(row["kind"]),
            catalog_key=str(row["catalog_key"]),
            name=None if row["name"] is None else str(row["name"]),
            description=None if row["description"] is None else str(row["description"]),
            source_type=str(row["source_type"]),
            member_count=int(row["member_count"]),
            metadata=dict(row["metadata"]) if isinstance(row["metadata"], dict) else {},
        )
        for row in rows
    )


def list_selection_profiles(
    *,
    is_active: bool | None = True,
    postgres: PostgresSettings | None = None,
) -> tuple[SelectionProfileSummary, ...]:
    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT
                    p.profile_id,
                    p.profile_key,
                    p.name,
                    p.description,
                    p.source_type,
                    p.is_active,
                    p.metadata,
                    (
                        SELECT COUNT(*)
                        FROM {INVENTORY_V2_SCHEMA}.selection_profile_item item
                        WHERE item.profile_id = p.profile_id
                    ) AS item_count
                FROM {INVENTORY_V2_SCHEMA}.selection_profile p
                WHERE (%s::boolean IS NULL OR p.is_active = %s::boolean)
                ORDER BY p.updated_at DESC, p.profile_id DESC
                """,
                (is_active, is_active),
            )
    return tuple(
        SelectionProfileSummary(
            profile_id=int(row["profile_id"]),
            profile_key=str(row["profile_key"]),
            name=str(row["name"]),
            description=None if row["description"] is None else str(row["description"]),
            source_type=str(row["source_type"]),
            is_active=bool(row["is_active"]),
            item_count=int(row["item_count"]),
            metadata=dict(row["metadata"]) if isinstance(row["metadata"], dict) else {},
        )
        for row in rows
    )


def search_inventory_entities(
    kind: str,
    *,
    query: str | None = None,
    catalog_key: str | None = None,
    bbox: dict[str, float] | None = None,
    region_codes: list[int] | None = None,
    subject_codes: list[int] | None = None,
    federal_districts: list[str] | None = None,
    grid_cell_ids: list[str] | None = None,
    is_active: bool | None = None,
    limit: int = 100,
    offset: int = 0,
    postgres: PostgresSettings | None = None,
) -> tuple[InventoryEntityRecord, ...]:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _KIND_ENTITY_CONFIG:
        raise ValueError(f"Unsupported inventory kind: {normalized_kind}")

    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()
    config = _KIND_ENTITY_CONFIG[normalized_kind]
    bbox_filter = _normalize_bbox(bbox)
    region_code_values = None if region_codes is None else [int(value) for value in region_codes]
    subject_code_values = None if subject_codes is None else [int(value) for value in subject_codes]
    district_values = None if federal_districts is None else _normalize_text_list(federal_districts)
    grid_values = None if grid_cell_ids is None else _normalize_text_list(grid_cell_ids)
    limit_value = max(1, min(int(limit), 1000))
    offset_value = max(int(offset), 0)

    join_sql = ""
    ordinal_select = "NULL::integer AS catalog_ordinal"
    params: list[Any] = []
    if catalog_key is not None:
        join_sql = f"""
            JOIN {INVENTORY_V2_SCHEMA}.selection_catalog c
              ON c.kind = %s AND c.catalog_key = %s
            JOIN {INVENTORY_V2_SCHEMA}.{config["member_table"]} m
              ON m.catalog_id = c.catalog_id
             AND m.{config["member_entity_column"]} = e.{config["entity_id_column"]}
        """
        ordinal_select = "m.ordinal AS catalog_ordinal"
        params.extend((normalized_kind, str(catalog_key)))

    where_sql, where_params = _search_query_parts(
        normalized_kind,
        config,
        query=query,
        bbox=bbox_filter,
        region_codes=region_code_values,
        subject_codes=subject_code_values,
        federal_districts=district_values,
        grid_cell_ids=grid_values,
        is_active=is_active,
    )
    params.extend(where_params)
    order_sql = (
        f"ORDER BY m.ordinal, e.name, e.{config['key_column']}"
        if catalog_key is not None
        else f"ORDER BY e.name, e.{config['key_column']}"
    )

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT
                    {config["search_select"]},
                    {ordinal_select}
                FROM {INVENTORY_V2_SCHEMA}.{config["table"]} e
                {join_sql}
                {where_sql}
                {order_sql}
                LIMIT %s
                OFFSET %s
                """,
                tuple(params + [limit_value, offset_value]),
            )
    return tuple(_inventory_entity_from_row(normalized_kind, row) for row in rows)


def count_inventory_entities(
    kind: str,
    *,
    query: str | None = None,
    catalog_key: str | None = None,
    bbox: dict[str, float] | None = None,
    region_codes: list[int] | None = None,
    subject_codes: list[int] | None = None,
    federal_districts: list[str] | None = None,
    grid_cell_ids: list[str] | None = None,
    is_active: bool | None = None,
    postgres: PostgresSettings | None = None,
) -> int:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _KIND_ENTITY_CONFIG:
        raise ValueError(f"Unsupported inventory kind: {normalized_kind}")

    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()
    config = _KIND_ENTITY_CONFIG[normalized_kind]
    bbox_filter = _normalize_bbox(bbox)
    region_code_values = None if region_codes is None else [int(value) for value in region_codes]
    subject_code_values = None if subject_codes is None else [int(value) for value in subject_codes]
    district_values = None if federal_districts is None else _normalize_text_list(federal_districts)
    grid_values = None if grid_cell_ids is None else _normalize_text_list(grid_cell_ids)

    join_sql = ""
    params: list[Any] = []
    if catalog_key is not None:
        join_sql = f"""
            JOIN {INVENTORY_V2_SCHEMA}.selection_catalog c
              ON c.kind = %s AND c.catalog_key = %s
            JOIN {INVENTORY_V2_SCHEMA}.{config["member_table"]} m
              ON m.catalog_id = c.catalog_id
             AND m.{config["member_entity_column"]} = e.{config["entity_id_column"]}
        """
        params.extend((normalized_kind, str(catalog_key)))

    where_sql, where_params = _search_query_parts(
        normalized_kind,
        config,
        query=query,
        bbox=bbox_filter,
        region_codes=region_code_values,
        subject_codes=subject_code_values,
        federal_districts=district_values,
        grid_cell_ids=grid_values,
        is_active=is_active,
    )
    params.extend(where_params)

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT COUNT(*) AS total_count
                FROM {INVENTORY_V2_SCHEMA}.{config["table"]} e
                {join_sql}
                {where_sql}
                """,
                tuple(params),
            )
    return int(rows[0]["total_count"]) if rows else 0


def coverage_inventory_entities(
    kind: str,
    *,
    query: str | None = None,
    catalog_key: str | None = None,
    bbox: dict[str, float] | None = None,
    region_codes: list[int] | None = None,
    subject_codes: list[int] | None = None,
    federal_districts: list[str] | None = None,
    grid_cell_ids: list[str] | None = None,
    is_active: bool | None = None,
    lat_bins: int = 10,
    lon_bins: int = 20,
    postgres: PostgresSettings | None = None,
) -> InventoryEntityCoverage:
    normalized_kind = str(kind).strip()
    if normalized_kind not in _KIND_ENTITY_CONFIG:
        raise ValueError(f"Unsupported inventory kind: {normalized_kind}")

    postgres = postgres or load_postgres_settings()
    config = _KIND_ENTITY_CONFIG[normalized_kind]
    lat_bin_count = max(1, min(int(lat_bins), 60))
    lon_bin_count = max(1, min(int(lon_bins), 120))

    if normalized_kind == "satellites":
        total_count = count_inventory_entities(
            normalized_kind,
            query=query,
            catalog_key=catalog_key,
            bbox=bbox,
            region_codes=region_codes,
            subject_codes=subject_codes,
            federal_districts=federal_districts,
            grid_cell_ids=grid_cell_ids,
            is_active=is_active,
            postgres=postgres,
        )
        return InventoryEntityCoverage(
            kind=normalized_kind,
            total_count=total_count,
            coord_count=0,
            lat_bins=lat_bin_count,
            lon_bins=lon_bin_count,
            note="satellites use TLE/orbit data and have no static inventory lat/lon",
        )

    psycopg = _require_psycopg()
    bbox_filter = _normalize_bbox(bbox)
    region_code_values = None if region_codes is None else [int(value) for value in region_codes]
    subject_code_values = None if subject_codes is None else [int(value) for value in subject_codes]
    district_values = None if federal_districts is None else _normalize_text_list(federal_districts)
    grid_values = None if grid_cell_ids is None else _normalize_text_list(grid_cell_ids)

    join_sql = ""
    params: list[Any] = []
    if catalog_key is not None:
        join_sql = f"""
            JOIN {INVENTORY_V2_SCHEMA}.selection_catalog c
              ON c.kind = %s AND c.catalog_key = %s
            JOIN {INVENTORY_V2_SCHEMA}.{config["member_table"]} m
              ON m.catalog_id = c.catalog_id
             AND m.{config["member_entity_column"]} = e.{config["entity_id_column"]}
        """
        params.extend((normalized_kind, str(catalog_key)))

    where_sql, where_params = _search_query_parts(
        normalized_kind,
        config,
        query=query,
        bbox=bbox_filter,
        region_codes=region_code_values,
        subject_codes=subject_code_values,
        federal_districts=district_values,
        grid_cell_ids=grid_values,
        is_active=is_active,
    )
    params.extend(where_params)
    filtered_sql = f"""
        SELECT e.lat::double precision AS lat, e.lon::double precision AS lon
        FROM {INVENTORY_V2_SCHEMA}.{config["table"]} e
        {join_sql}
        {where_sql}
    """

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                WITH filtered AS (
                    {filtered_sql}
                )
                SELECT
                    COUNT(*) AS total_count,
                    COUNT(*) FILTER (WHERE lat IS NOT NULL AND lon IS NOT NULL) AS coord_count,
                    MIN(lat) FILTER (WHERE lat IS NOT NULL AND lon IS NOT NULL) AS lat_min,
                    MAX(lat) FILTER (WHERE lat IS NOT NULL AND lon IS NOT NULL) AS lat_max,
                    MIN(lon) FILTER (WHERE lat IS NOT NULL AND lon IS NOT NULL) AS lon_min,
                    MAX(lon) FILTER (WHERE lat IS NOT NULL AND lon IS NOT NULL) AS lon_max
                FROM filtered
                """,
                tuple(params),
            )
            summary = rows[0] if rows else {}
            total_count = int(summary.get("total_count") or 0)
            coord_count = int(summary.get("coord_count") or 0)
            if coord_count <= 0:
                return InventoryEntityCoverage(
                    kind=normalized_kind,
                    total_count=total_count,
                    coord_count=0,
                    lat_bins=lat_bin_count,
                    lon_bins=lon_bin_count,
                    note="no inventory rows with lat/lon matched current filters",
                )

            lat_min = float(summary["lat_min"])
            lat_max = float(summary["lat_max"])
            lon_min = float(summary["lon_min"])
            lon_max = float(summary["lon_max"])
            lat_span = max(0.0, lat_max - lat_min)
            lon_span = max(0.0, lon_max - lon_min)
            bucket_rows = _fetch_rows(
                cursor,
                f"""
                WITH filtered AS (
                    {filtered_sql}
                ),
                bounds AS (
                    SELECT
                        %s::double precision AS lat_min,
                        %s::double precision AS lon_min,
                        %s::double precision AS lat_span,
                        %s::double precision AS lon_span,
                        %s::integer AS lat_bins,
                        %s::integer AS lon_bins
                ),
                bucketed AS (
                    SELECT
                        CASE
                            WHEN b.lat_span <= 0 THEN 0
                            ELSE LEAST(b.lat_bins - 1, GREATEST(0, FLOOR(((f.lat - b.lat_min) / b.lat_span) * b.lat_bins)::integer))
                        END AS lat_bucket,
                        CASE
                            WHEN b.lon_span <= 0 THEN 0
                            ELSE LEAST(b.lon_bins - 1, GREATEST(0, FLOOR(((f.lon - b.lon_min) / b.lon_span) * b.lon_bins)::integer))
                        END AS lon_bucket,
                        f.lat,
                        f.lon
                    FROM filtered f
                    CROSS JOIN bounds b
                    WHERE f.lat IS NOT NULL AND f.lon IS NOT NULL
                )
                SELECT
                    lat_bucket,
                    lon_bucket,
                    COUNT(*) AS count,
                    AVG(lat) AS lat,
                    AVG(lon) AS lon,
                    MIN(lat) AS lat_min,
                    MAX(lat) AS lat_max,
                    MIN(lon) AS lon_min,
                    MAX(lon) AS lon_max
                FROM bucketed
                GROUP BY lat_bucket, lon_bucket
                ORDER BY lat_bucket, lon_bucket
                """,
                tuple(params + [lat_min, lon_min, lat_span, lon_span, lat_bin_count, lon_bin_count]),
            )

    return InventoryEntityCoverage(
        kind=normalized_kind,
        total_count=total_count,
        coord_count=coord_count,
        lat_bins=lat_bin_count,
        lon_bins=lon_bin_count,
        bounds={
            "lat_min": lat_min,
            "lat_max": lat_max,
            "lon_min": lon_min,
            "lon_max": lon_max,
        },
        buckets=tuple(
            InventoryEntityCoverageBucket(
                lat_bucket=int(row["lat_bucket"]),
                lon_bucket=int(row["lon_bucket"]),
                count=int(row["count"]),
                lat=float(row["lat"]),
                lon=float(row["lon"]),
                lat_min=float(row["lat_min"]),
                lat_max=float(row["lat_max"]),
                lon_min=float(row["lon_min"]),
                lon_max=float(row["lon_max"]),
            )
            for row in bucket_rows
        ),
    )


def search_inventory_entity_page(
    kind: str,
    *,
    query: str | None = None,
    catalog_key: str | None = None,
    bbox: dict[str, float] | None = None,
    region_codes: list[int] | None = None,
    subject_codes: list[int] | None = None,
    federal_districts: list[str] | None = None,
    grid_cell_ids: list[str] | None = None,
    is_active: bool | None = None,
    limit: int = 100,
    offset: int = 0,
    postgres: PostgresSettings | None = None,
) -> InventoryEntitySearchPage:
    items = search_inventory_entities(
        kind,
        query=query,
        catalog_key=catalog_key,
        bbox=bbox,
        region_codes=region_codes,
        subject_codes=subject_codes,
        federal_districts=federal_districts,
        grid_cell_ids=grid_cell_ids,
        is_active=is_active,
        limit=limit,
        offset=offset,
        postgres=postgres,
    )
    total_count = count_inventory_entities(
        kind,
        query=query,
        catalog_key=catalog_key,
        bbox=bbox,
        region_codes=region_codes,
        subject_codes=subject_codes,
        federal_districts=federal_districts,
        grid_cell_ids=grid_cell_ids,
        is_active=is_active,
        postgres=postgres,
    )
    return InventoryEntitySearchPage(
        kind=str(kind),
        items=items,
        total_count=total_count,
        limit=max(1, min(int(limit), 1000)),
        offset=max(int(offset), 0),
    )


def resolve_selection_items(
    selection: InventorySelectionSpec,
    postgres: PostgresSettings | None = None,
) -> tuple[InventorySelectionItem, ...]:
    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()
    if str(selection.source) not in {"postgres_v2", "inventory_explicit"}:
        raise ValueError(f"Unsupported inventory selection source: {selection.source}")

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            return _selection_items(cursor, selection)


def create_selection_profile(
    draft: SelectionProfileDraft,
    postgres: PostgresSettings | None = None,
) -> SelectionProfileRecord:
    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()
    ensure_schema_v2(postgres)

    profile_key = str(draft.profile_key or f"profile-{uuid4().hex}")
    description = None if draft.description is None else str(draft.description)
    items = _ordered_items(tuple(draft.items))

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {INVENTORY_V2_SCHEMA}.selection_profile (
                    profile_key,
                    name,
                    description,
                    source_type,
                    metadata
                ) VALUES (%s, %s, %s, %s, %s)
                RETURNING profile_id
                """,
                (
                    profile_key,
                    str(draft.name),
                    description,
                    str(draft.source_type),
                    Jsonb(dict(draft.metadata)),
                ),
            )
            profile_id = int(cursor.fetchone()[0])

            for item in items:
                cursor.execute(
                    f"""
                    INSERT INTO {INVENTORY_V2_SCHEMA}.selection_profile_item (
                        profile_id,
                        kind,
                        entity_key,
                        ordinal,
                        enabled,
                        metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        profile_id,
                        str(item.kind),
                        str(item.entity_key),
                        int(item.ordinal),
                        bool(item.enabled),
                        Jsonb(dict(item.metadata)),
                    ),
                )
        connection.commit()

    return SelectionProfileRecord(
        profile_id=profile_id,
        profile_key=profile_key,
        name=str(draft.name),
        description=description,
        source_type=str(draft.source_type),
        is_active=True,
        metadata=dict(draft.metadata),
        items=items,
    )


def replace_selection_profile(
    profile_id: int,
    draft: SelectionProfileDraft,
    postgres: PostgresSettings | None = None,
) -> SelectionProfileRecord:
    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()
    ensure_schema_v2(postgres)

    items = _ordered_items(tuple(draft.items))
    description = None if draft.description is None else str(draft.description)

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT profile_key
                FROM {INVENTORY_V2_SCHEMA}.selection_profile
                WHERE profile_id = %s
                """,
                (int(profile_id),),
            )
            if not rows:
                raise ValueError(f"Selection profile {profile_id} was not found")

            resolved_profile_key = str(draft.profile_key or rows[0]["profile_key"])
            cursor.execute(
                f"""
                UPDATE {INVENTORY_V2_SCHEMA}.selection_profile
                SET profile_key = %s,
                    name = %s,
                    description = %s,
                    source_type = %s,
                    metadata = %s,
                    updated_at = now()
                WHERE profile_id = %s
                """,
                (
                    resolved_profile_key,
                    str(draft.name),
                    description,
                    str(draft.source_type),
                    Jsonb(dict(draft.metadata)),
                    int(profile_id),
                ),
            )
            cursor.execute(
                f"DELETE FROM {INVENTORY_V2_SCHEMA}.selection_profile_item WHERE profile_id = %s",
                (int(profile_id),),
            )
            for item in items:
                cursor.execute(
                    f"""
                    INSERT INTO {INVENTORY_V2_SCHEMA}.selection_profile_item (
                        profile_id,
                        kind,
                        entity_key,
                        ordinal,
                        enabled,
                        metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        int(profile_id),
                        str(item.kind),
                        str(item.entity_key),
                        int(item.ordinal),
                        bool(item.enabled),
                        Jsonb(dict(item.metadata)),
                    ),
                )
        connection.commit()

    return load_selection_profile(int(profile_id), postgres=postgres)


def load_run_snapshot(
    snapshot_id: int,
    postgres: PostgresSettings | None = None,
) -> RunSnapshotRecord:
    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT snapshot_id, request_key, name, profile_id AS selection_profile_id, source_type, status, metadata
                FROM {INVENTORY_V2_SCHEMA}.simulation_request_snapshot
                WHERE snapshot_id = %s
                """,
                (int(snapshot_id),),
            )
            if not rows:
                raise ValueError(f"Run snapshot {snapshot_id} was not found")
            row = rows[0]
            return RunSnapshotRecord(
                snapshot_id=int(row["snapshot_id"]),
                request_key=str(row["request_key"]),
                name=str(row["name"]),
                selection_profile_id=None
                if row["selection_profile_id"] is None
                else int(row["selection_profile_id"]),
                source_type=str(row["source_type"]),
                status=str(row["status"]),
                metadata=dict(row["metadata"]) if isinstance(row["metadata"], dict) else {},
                items=_snapshot_items(cursor, int(snapshot_id)),
            )


def update_run_snapshot_status(
    snapshot_id: int,
    *,
    status: str,
    metadata_patch: dict[str, Any] | None = None,
    postgres: PostgresSettings | None = None,
) -> RunSnapshotRecord:
    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            rows = _fetch_rows(
                cursor,
                f"""
                SELECT metadata
                FROM {INVENTORY_V2_SCHEMA}.simulation_request_snapshot
                WHERE snapshot_id = %s
                """,
                (int(snapshot_id),),
            )
            if not rows:
                raise ValueError(f"Run snapshot {snapshot_id} was not found")

            next_metadata = dict(rows[0]["metadata"]) if isinstance(rows[0]["metadata"], dict) else {}
            if metadata_patch:
                next_metadata.update(dict(metadata_patch))

            cursor.execute(
                f"""
                UPDATE {INVENTORY_V2_SCHEMA}.simulation_request_snapshot
                SET status = %s,
                    metadata = %s
                WHERE snapshot_id = %s
                """,
                (
                    str(status),
                    Jsonb(next_metadata),
                    int(snapshot_id),
                ),
            )
        connection.commit()

    return load_run_snapshot(int(snapshot_id), postgres=postgres)


def create_run_snapshot_from_selection(
    *,
    name: str,
    selection: InventorySelectionSpec,
    postgres: PostgresSettings | None = None,
    source_type: str = "manual",
    metadata: dict[str, Any] | None = None,
) -> RunSnapshotRecord:
    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg_with_jsonb()
    ensure_schema_v2(postgres)

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            items = _selection_items(cursor, selection)
            if not items:
                raise ValueError("Run snapshot cannot be created from an empty inventory selection")

            request_key = f"snapshot-{uuid4().hex}"
            payload_metadata = dict(metadata or {})
            payload_metadata.setdefault("selection_source", selection.source)
            if selection.run_snapshot_id is not None:
                payload_metadata.setdefault("derived_from_snapshot_id", int(selection.run_snapshot_id))
            cursor.execute(
                f"""
                INSERT INTO {INVENTORY_V2_SCHEMA}.simulation_request_snapshot (
                    request_key,
                    profile_id,
                    name,
                    source_type,
                    metadata
                ) VALUES (%s, %s, %s, %s, %s)
                RETURNING snapshot_id
                """,
                (
                    request_key,
                    None if selection.selection_profile_id is None else int(selection.selection_profile_id),
                    str(name),
                    str(source_type),
                    Jsonb(payload_metadata),
                ),
            )
            snapshot_id = int(cursor.fetchone()[0])

            for item in items:
                cursor.execute(
                    f"""
                    INSERT INTO {INVENTORY_V2_SCHEMA}.simulation_request_snapshot_item (
                        snapshot_id,
                        kind,
                        entity_key,
                        ordinal,
                        metadata
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        snapshot_id,
                        str(item.kind),
                        str(item.entity_key),
                        int(item.ordinal),
                        Jsonb(dict(item.metadata)),
                    ),
                )
        connection.commit()

    return load_run_snapshot(snapshot_id, postgres=postgres)


def load_inventory_bundle_from_selection(
    selection: InventorySelectionSpec,
    postgres: PostgresSettings | None = None,
) -> InventoryBundle:
    postgres = postgres or load_postgres_settings()
    psycopg = _require_psycopg()
    if str(selection.source) not in {"postgres_v2", "inventory_explicit"}:
        raise ValueError(f"Unsupported inventory selection source: {selection.source}")

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            items = _selection_items(cursor, selection)
            return _bundle_from_items(cursor, items, source=str(selection.source))
