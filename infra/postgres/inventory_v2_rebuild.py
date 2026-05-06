from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .inventory_store import INVENTORY_SCHEMA, INVENTORY_V2_SCHEMA
from .settings import PostgresSettings, load_postgres_settings
from .theoretical_subscribers_seed import (
    THEORETICAL_SUBSCRIBER_ASSET_PATH,
    THEORETICAL_SUBSCRIBER_CATALOG_KEY,
    THEORETICAL_SUBSCRIBER_SEED_VERSION,
    load_theoretical_subscriber_seed_rows,
)


IGNORED_METADATA_KEYS = frozenset(
    {
        "collection_key",
        "legacy_ordinal",
        "legacy_set",
        "ordinal",
        "row_index",
        "set_key",
    }
)
JSON_COLUMNS = frozenset({"metadata", "limits", "resource_limits", "allowed_link_types"})
THEORETICAL_SUBSCRIBER_COLUMNS = (
    "name",
    "lat",
    "lon",
    "site_alt_m",
    "ground_terminal_profile_key",
    "subject_code",
    "subject_name",
    "federal_district",
    "grid_cell_id",
    "seed_version",
    "is_active",
    "metadata",
)
PROFILE_SEED_VERSION = "inventory_v2_explicit_profiles_v1"
THEORETICAL_SUBSCRIBER_GROUND_PROFILE_KEY = "theoretical_subscriber_ground_default_v1"

GROUND_PROFILE_DEFAULTS: dict[str, dict[str, Any]] = {
    "gateway": {
        "tx_center_frequency_ghz": 27.5,
        "tx_bandwidth_mhz": 1200.0,
        "tx_polarization_deg": 45.0,
        "tx_antenna_diameter_m": 3.7,
        "tx_antenna_gain_dbi": 52.0,
        "tx_waveguide_loss_db": 1.0,
        "tx_power_dbw": 18.0,
        "rx_center_frequency_ghz": 17.8,
        "rx_bandwidth_mhz": 1200.0,
        "rx_polarization_deg": 45.0,
        "rx_antenna_diameter_m": 3.7,
        "rx_antenna_gain_dbi": 50.0,
        "rx_waveguide_loss_db": 1.0,
        "lna_noise_temperature_k": 85.0,
        "rolloff": 0.2,
        "lm_db": 0.5,
        "if_to_rf_degradation_db": 0.8,
        "rain_probability_percent": 0.1,
        "off_axis_loss_db_per_rad": 2.5,
    },
    "vsat": {
        "tx_center_frequency_ghz": 29.5,
        "tx_bandwidth_mhz": 250.0,
        "tx_polarization_deg": 45.0,
        "tx_antenna_diameter_m": 1.2,
        "tx_antenna_gain_dbi": 42.0,
        "tx_waveguide_loss_db": 0.7,
        "tx_power_dbw": 8.0,
        "rx_center_frequency_ghz": 19.5,
        "rx_bandwidth_mhz": 250.0,
        "rx_polarization_deg": 45.0,
        "rx_antenna_diameter_m": 1.2,
        "rx_antenna_gain_dbi": 40.0,
        "rx_waveguide_loss_db": 0.7,
        "lna_noise_temperature_k": 140.0,
        "rolloff": 0.2,
        "lm_db": 0.5,
        "if_to_rf_degradation_db": 0.8,
        "rain_probability_percent": 0.1,
        "off_axis_loss_db_per_rad": 5.0,
    },
    "theoretical_subscriber": {
        "tx_center_frequency_ghz": 29.5,
        "tx_bandwidth_mhz": 150.0,
        "tx_polarization_deg": 45.0,
        "tx_antenna_diameter_m": 0.75,
        "tx_antenna_gain_dbi": 39.0,
        "tx_waveguide_loss_db": 0.8,
        "tx_power_dbw": 5.0,
        "rx_center_frequency_ghz": 19.5,
        "rx_bandwidth_mhz": 150.0,
        "rx_polarization_deg": 45.0,
        "rx_antenna_diameter_m": 0.75,
        "rx_antenna_gain_dbi": 37.0,
        "rx_waveguide_loss_db": 0.8,
        "lna_noise_temperature_k": 150.0,
        "rolloff": 0.2,
        "lm_db": 0.5,
        "if_to_rf_degradation_db": 0.8,
        "rain_probability_percent": 0.1,
        "off_axis_loss_db_per_rad": 6.0,
    },
    "target": {
        "tx_center_frequency_ghz": 29.5,
        "tx_bandwidth_mhz": 100.0,
        "tx_polarization_deg": 45.0,
        "tx_antenna_diameter_m": 0.6,
        "tx_antenna_gain_dbi": 36.0,
        "tx_waveguide_loss_db": 0.8,
        "tx_power_dbw": 3.0,
        "rx_center_frequency_ghz": 19.5,
        "rx_bandwidth_mhz": 100.0,
        "rx_polarization_deg": 45.0,
        "rx_antenna_diameter_m": 0.6,
        "rx_antenna_gain_dbi": 35.0,
        "rx_waveguide_loss_db": 0.8,
        "lna_noise_temperature_k": 160.0,
        "rolloff": 0.2,
        "lm_db": 0.5,
        "if_to_rf_degradation_db": 0.8,
        "rain_probability_percent": 0.1,
        "off_axis_loss_db_per_rad": 7.0,
    },
}

RELAY_PROFILE_DEFAULTS: dict[tuple[str, str], dict[str, Any]] = {
    ("satellites", "user"): {
        "gt_dbk": 15.0,
        "eirp_sat_dbw": 58.0,
        "sfd_dbw_m2": -118.0,
        "ibo_db": 5.5,
        "obo_db": 4.5,
        "npr_db": 18.0,
        "tx_center_frequency_ghz": 19.5,
        "tx_bandwidth_mhz": 250.0,
        "tx_polarization_deg": 45.0,
        "tx_antenna_diameter_m": 1.6,
        "tx_antenna_gain_dbi": 43.0,
        "tx_waveguide_loss_db": 0.6,
        "rx_center_frequency_ghz": 29.5,
        "rx_bandwidth_mhz": 250.0,
        "rx_polarization_deg": 45.0,
        "rx_antenna_diameter_m": 1.6,
        "rx_antenna_gain_dbi": 41.0,
        "rx_waveguide_loss_db": 0.6,
        "rx_noise_temperature_k": 290.0,
        "off_axis_loss_db_per_rad": 4.0,
    },
    ("satellites", "feeder"): {
        "gt_dbk": 18.0,
        "eirp_sat_dbw": 62.0,
        "sfd_dbw_m2": -120.0,
        "ibo_db": 4.0,
        "obo_db": 3.5,
        "npr_db": 20.0,
        "tx_center_frequency_ghz": 17.8,
        "tx_bandwidth_mhz": 1200.0,
        "tx_polarization_deg": 45.0,
        "tx_antenna_diameter_m": 2.2,
        "tx_antenna_gain_dbi": 48.0,
        "tx_waveguide_loss_db": 0.5,
        "rx_center_frequency_ghz": 27.5,
        "rx_bandwidth_mhz": 1200.0,
        "rx_polarization_deg": 45.0,
        "rx_antenna_diameter_m": 2.2,
        "rx_antenna_gain_dbi": 46.0,
        "rx_waveguide_loss_db": 0.5,
        "rx_noise_temperature_k": 250.0,
        "off_axis_loss_db_per_rad": 3.0,
    },
    ("haps", "user"): {
        "gt_dbk": 12.0,
        "eirp_sat_dbw": 48.0,
        "sfd_dbw_m2": -110.0,
        "ibo_db": 4.0,
        "obo_db": 3.0,
        "npr_db": 16.0,
        "tx_center_frequency_ghz": 19.5,
        "tx_bandwidth_mhz": 200.0,
        "tx_polarization_deg": 45.0,
        "tx_antenna_diameter_m": 0.8,
        "tx_antenna_gain_dbi": 36.0,
        "tx_waveguide_loss_db": 0.5,
        "rx_center_frequency_ghz": 29.5,
        "rx_bandwidth_mhz": 200.0,
        "rx_polarization_deg": 45.0,
        "rx_antenna_diameter_m": 0.8,
        "rx_antenna_gain_dbi": 34.0,
        "rx_waveguide_loss_db": 0.5,
        "rx_noise_temperature_k": 260.0,
        "off_axis_loss_db_per_rad": 5.0,
    },
    ("haps", "feeder"): {
        "gt_dbk": 14.0,
        "eirp_sat_dbw": 52.0,
        "sfd_dbw_m2": -113.0,
        "ibo_db": 4.0,
        "obo_db": 3.0,
        "npr_db": 17.0,
        "tx_center_frequency_ghz": 17.8,
        "tx_bandwidth_mhz": 800.0,
        "tx_polarization_deg": 45.0,
        "tx_antenna_diameter_m": 1.2,
        "tx_antenna_gain_dbi": 40.0,
        "tx_waveguide_loss_db": 0.5,
        "rx_center_frequency_ghz": 27.5,
        "rx_bandwidth_mhz": 800.0,
        "rx_polarization_deg": 45.0,
        "rx_antenna_diameter_m": 1.2,
        "rx_antenna_gain_dbi": 38.0,
        "rx_waveguide_loss_db": 0.5,
        "rx_noise_temperature_k": 240.0,
        "off_axis_loss_db_per_rad": 4.0,
    },
}

RELAY_PROFILE_KEY_PREFIX = {
    "satellites": "satellite",
    "haps": "haps",
}

CURATED_GROUND_PROFILE_VARIANTS: dict[str, dict[str, dict[str, Any]]] = {
    "gateway": {
        "geo_hub": {
            "tx_center_frequency_ghz": 27.5,
            "tx_bandwidth_mhz": 2500.0,
            "tx_polarization_deg": 45.0,
            "tx_antenna_diameter_m": 7.3,
            "tx_antenna_gain_dbi": 59.0,
            "tx_waveguide_loss_db": 0.5,
            "tx_power_dbw": 24.0,
            "rx_center_frequency_ghz": 17.8,
            "rx_bandwidth_mhz": 2500.0,
            "rx_polarization_deg": 45.0,
            "rx_antenna_diameter_m": 7.3,
            "rx_antenna_gain_dbi": 57.0,
            "rx_waveguide_loss_db": 0.5,
            "lna_noise_temperature_k": 65.0,
            "rolloff": 0.2,
            "lm_db": 0.3,
            "if_to_rf_degradation_db": 0.5,
            "rain_probability_percent": 0.1,
            "off_axis_loss_db_per_rad": 1.5,
        },
        "regional": {
            "tx_center_frequency_ghz": 27.5,
            "tx_bandwidth_mhz": 800.0,
            "tx_polarization_deg": 45.0,
            "tx_antenna_diameter_m": 2.4,
            "tx_antenna_gain_dbi": 48.0,
            "tx_waveguide_loss_db": 0.9,
            "tx_power_dbw": 15.0,
            "rx_center_frequency_ghz": 17.8,
            "rx_bandwidth_mhz": 800.0,
            "rx_polarization_deg": 45.0,
            "rx_antenna_diameter_m": 2.4,
            "rx_antenna_gain_dbi": 46.0,
            "rx_waveguide_loss_db": 0.9,
            "lna_noise_temperature_k": 95.0,
            "rolloff": 0.2,
            "lm_db": 0.7,
            "if_to_rf_degradation_db": 0.9,
            "rain_probability_percent": 0.1,
            "off_axis_loss_db_per_rad": 3.0,
        },
    },
    "vsat": {
        "compact": {
            "tx_center_frequency_ghz": 29.5,
            "tx_bandwidth_mhz": 120.0,
            "tx_polarization_deg": 45.0,
            "tx_antenna_diameter_m": 0.75,
            "tx_antenna_gain_dbi": 39.0,
            "tx_waveguide_loss_db": 0.8,
            "tx_power_dbw": 5.5,
            "rx_center_frequency_ghz": 19.5,
            "rx_bandwidth_mhz": 120.0,
            "rx_polarization_deg": 45.0,
            "rx_antenna_diameter_m": 0.75,
            "rx_antenna_gain_dbi": 37.5,
            "rx_waveguide_loss_db": 0.8,
            "lna_noise_temperature_k": 155.0,
            "rolloff": 0.2,
            "lm_db": 0.6,
            "if_to_rf_degradation_db": 0.9,
            "rain_probability_percent": 0.1,
            "off_axis_loss_db_per_rad": 6.0,
        },
        "highrate": {
            "tx_center_frequency_ghz": 29.5,
            "tx_bandwidth_mhz": 500.0,
            "tx_polarization_deg": 45.0,
            "tx_antenna_diameter_m": 1.8,
            "tx_antenna_gain_dbi": 45.0,
            "tx_waveguide_loss_db": 0.6,
            "tx_power_dbw": 10.0,
            "rx_center_frequency_ghz": 19.5,
            "rx_bandwidth_mhz": 500.0,
            "rx_polarization_deg": 45.0,
            "rx_antenna_diameter_m": 1.8,
            "rx_antenna_gain_dbi": 43.5,
            "rx_waveguide_loss_db": 0.6,
            "lna_noise_temperature_k": 125.0,
            "rolloff": 0.2,
            "lm_db": 0.4,
            "if_to_rf_degradation_db": 0.7,
            "rain_probability_percent": 0.1,
            "off_axis_loss_db_per_rad": 4.0,
        },
    },
    "theoretical_subscriber": {
        "plus": {
            "tx_center_frequency_ghz": 29.5,
            "tx_bandwidth_mhz": 200.0,
            "tx_polarization_deg": 45.0,
            "tx_antenna_diameter_m": 1.0,
            "tx_antenna_gain_dbi": 40.5,
            "tx_waveguide_loss_db": 0.8,
            "tx_power_dbw": 6.5,
            "rx_center_frequency_ghz": 19.5,
            "rx_bandwidth_mhz": 200.0,
            "rx_polarization_deg": 45.0,
            "rx_antenna_diameter_m": 1.0,
            "rx_antenna_gain_dbi": 39.0,
            "rx_waveguide_loss_db": 0.8,
            "lna_noise_temperature_k": 145.0,
            "rolloff": 0.2,
            "lm_db": 0.5,
            "if_to_rf_degradation_db": 0.8,
            "rain_probability_percent": 0.1,
            "off_axis_loss_db_per_rad": 5.5,
        },
    },
}

CURATED_RELAY_PROFILE_VARIANTS: dict[tuple[str, str], dict[str, dict[str, Any]]] = {
    ("satellites", "user"): {
        "geo_highpower": {
            "gt_dbk": 18.0,
            "eirp_sat_dbw": 62.0,
            "sfd_dbw_m2": -119.0,
            "ibo_db": 4.5,
            "obo_db": 3.5,
            "npr_db": 20.0,
            "tx_center_frequency_ghz": 19.5,
            "tx_bandwidth_mhz": 500.0,
            "tx_polarization_deg": 45.0,
            "tx_antenna_diameter_m": 2.0,
            "tx_antenna_gain_dbi": 45.5,
            "tx_waveguide_loss_db": 0.4,
            "rx_center_frequency_ghz": 29.5,
            "rx_bandwidth_mhz": 500.0,
            "rx_polarization_deg": 45.0,
            "rx_antenna_diameter_m": 2.0,
            "rx_antenna_gain_dbi": 43.5,
            "rx_waveguide_loss_db": 0.4,
            "rx_noise_temperature_k": 260.0,
            "off_axis_loss_db_per_rad": 2.0,
        },
        "highpower": {
            "gt_dbk": 16.5,
            "eirp_sat_dbw": 61.0,
            "sfd_dbw_m2": -119.0,
            "ibo_db": 5.0,
            "obo_db": 4.0,
            "npr_db": 19.0,
            "tx_center_frequency_ghz": 19.5,
            "tx_bandwidth_mhz": 350.0,
            "tx_polarization_deg": 45.0,
            "tx_antenna_diameter_m": 1.8,
            "tx_antenna_gain_dbi": 44.5,
            "tx_waveguide_loss_db": 0.5,
            "rx_center_frequency_ghz": 29.5,
            "rx_bandwidth_mhz": 350.0,
            "rx_polarization_deg": 45.0,
            "rx_antenna_diameter_m": 1.8,
            "rx_antenna_gain_dbi": 42.5,
            "rx_waveguide_loss_db": 0.5,
            "rx_noise_temperature_k": 275.0,
            "off_axis_loss_db_per_rad": 3.5,
        },
    },
    ("satellites", "feeder"): {
        "highcap": {
            "gt_dbk": 19.5,
            "eirp_sat_dbw": 64.0,
            "sfd_dbw_m2": -121.0,
            "ibo_db": 3.5,
            "obo_db": 3.0,
            "npr_db": 21.0,
            "tx_center_frequency_ghz": 17.8,
            "tx_bandwidth_mhz": 1600.0,
            "tx_polarization_deg": 45.0,
            "tx_antenna_diameter_m": 2.6,
            "tx_antenna_gain_dbi": 49.5,
            "tx_waveguide_loss_db": 0.4,
            "rx_center_frequency_ghz": 27.5,
            "rx_bandwidth_mhz": 1600.0,
            "rx_polarization_deg": 45.0,
            "rx_antenna_diameter_m": 2.6,
            "rx_antenna_gain_dbi": 47.5,
            "rx_waveguide_loss_db": 0.4,
            "rx_noise_temperature_k": 235.0,
            "off_axis_loss_db_per_rad": 2.5,
        },
    },
    ("haps", "user"): {
        "dense": {
            "gt_dbk": 13.0,
            "eirp_sat_dbw": 50.0,
            "sfd_dbw_m2": -111.0,
            "ibo_db": 3.8,
            "obo_db": 2.8,
            "npr_db": 16.5,
            "tx_center_frequency_ghz": 19.5,
            "tx_bandwidth_mhz": 280.0,
            "tx_polarization_deg": 45.0,
            "tx_antenna_diameter_m": 1.0,
            "tx_antenna_gain_dbi": 37.5,
            "tx_waveguide_loss_db": 0.4,
            "rx_center_frequency_ghz": 29.5,
            "rx_bandwidth_mhz": 280.0,
            "rx_polarization_deg": 45.0,
            "rx_antenna_diameter_m": 1.0,
            "rx_antenna_gain_dbi": 35.5,
            "rx_waveguide_loss_db": 0.4,
            "rx_noise_temperature_k": 250.0,
            "off_axis_loss_db_per_rad": 4.5,
        },
    },
    ("haps", "feeder"): {
        "highcap": {
            "gt_dbk": 15.0,
            "eirp_sat_dbw": 54.0,
            "sfd_dbw_m2": -114.0,
            "ibo_db": 3.8,
            "obo_db": 2.8,
            "npr_db": 18.0,
            "tx_center_frequency_ghz": 17.8,
            "tx_bandwidth_mhz": 1000.0,
            "tx_polarization_deg": 45.0,
            "tx_antenna_diameter_m": 1.4,
            "tx_antenna_gain_dbi": 41.5,
            "tx_waveguide_loss_db": 0.4,
            "rx_center_frequency_ghz": 27.5,
            "rx_bandwidth_mhz": 1000.0,
            "rx_polarization_deg": 45.0,
            "rx_antenna_diameter_m": 1.4,
            "rx_antenna_gain_dbi": 39.5,
            "rx_waveguide_loss_db": 0.4,
            "rx_noise_temperature_k": 225.0,
            "off_axis_loss_db_per_rad": 3.5,
        },
    },
}

DEFAULT_PROFILE_ASSIGNMENT_RULES: tuple[dict[str, Any], ...] = (
    {
        "rule_key": "gateways-ground-default",
        "name": "Gateways default ground profile",
        "kind": "gateways",
        "ground_terminal_profile_key": "gateway_ground_default_v1",
        "priority": 100,
    },
    {
        "rule_key": "targets-ground-default",
        "name": "Targets default ground profile",
        "kind": "targets",
        "ground_terminal_profile_key": "target_ground_default_v1",
        "priority": 110,
    },
    {
        "rule_key": "vsats-ground-default",
        "name": "VSAT default ground profile",
        "kind": "vsats",
        "ground_terminal_profile_key": "vsat_ground_default_v1",
        "priority": 120,
    },
    {
        "rule_key": "vsats-ground-highrate-remote-north",
        "name": "VSAT highrate profile for remote northern regions",
        "kind": "vsats",
        "region_codes": [0, 4, 11, 44, 77, 98, 71100, 71140],
        "ground_terminal_profile_key": "vsat_ground_highrate_v1",
        "priority": 220,
    },
    {
        "rule_key": "theoretical-subscribers-ground-default",
        "name": "Theoretical subscribers default ground profile",
        "kind": "theoretical_subscribers",
        "ground_terminal_profile_key": THEORETICAL_SUBSCRIBER_GROUND_PROFILE_KEY,
        "priority": 130,
    },
    {
        "rule_key": "theoretical-subscribers-ground-plus-arctic",
        "name": "Theoretical subscribers plus profile for Arctic coverage",
        "kind": "theoretical_subscribers",
        "federal_districts": ["Арктика"],
        "ground_terminal_profile_key": "theoretical_subscriber_ground_plus_v1",
        "priority": 220,
    },
    {
        "rule_key": "theoretical-subscribers-ground-plus-far-east",
        "name": "Theoretical subscribers plus profile for Far East coverage",
        "kind": "theoretical_subscribers",
        "federal_districts": ["Дальневосточный федеральный округ"],
        "ground_terminal_profile_key": "theoretical_subscriber_ground_plus_v1",
        "priority": 230,
    },
    {
        "rule_key": "theoretical-subscribers-ground-plus-siberia",
        "name": "Theoretical subscribers plus profile for Siberian coverage",
        "kind": "theoretical_subscribers",
        "federal_districts": ["Сибирский федеральный округ"],
        "ground_terminal_profile_key": "theoretical_subscriber_ground_plus_v1",
        "priority": 240,
    },
    {
        "rule_key": "satellites-relay-default",
        "name": "Satellites default relay profiles",
        "kind": "satellites",
        "user_beam_profile_key": "satellite_user_default_v1",
        "feeder_beam_profile_key": "satellite_feeder_default_v1",
        "priority": 140,
    },
    {
        "rule_key": "satellites-relay-starlink-highcap",
        "name": "Starlink satellites high-capacity relay profiles",
        "kind": "satellites",
        "catalog_key": "starlink",
        "user_beam_profile_key": "satellite_user_highpower_v1",
        "feeder_beam_profile_key": "satellite_feeder_highcap_v1",
        "priority": 220,
    },
    {
        "rule_key": "satellites-relay-skif-highcap",
        "name": "Skif satellites high-capacity relay profiles",
        "kind": "satellites",
        "catalog_key": "skif",
        "user_beam_profile_key": "satellite_user_highpower_v1",
        "feeder_beam_profile_key": "satellite_feeder_highcap_v1",
        "priority": 230,
    },
    {
        "rule_key": "satellites-relay-geo-russia-highcap",
        "name": "Russia GEO satellites wide-footprint relay profiles",
        "kind": "satellites",
        "catalog_key": "geo-russia",
        "user_beam_profile_key": "satellite_user_geo_highpower_v1",
        "feeder_beam_profile_key": "satellite_feeder_highcap_v1",
        "priority": 240,
    },
    {
        "rule_key": "haps-relay-default",
        "name": "HAPS default relay profiles",
        "kind": "haps",
        "user_beam_profile_key": "haps_user_default_v1",
        "feeder_beam_profile_key": "haps_feeder_default_v1",
        "priority": 150,
    },
    {
        "rule_key": "haps-relay-dense-catalog-15",
        "name": "Dense HAPS relay profiles for 15-node catalog",
        "kind": "haps",
        "catalog_key": "15",
        "user_beam_profile_key": "haps_user_dense_v1",
        "feeder_beam_profile_key": "haps_feeder_highcap_v1",
        "priority": 220,
    },
)


@dataclass(frozen=True, slots=True)
class InventoryKindConfig:
    kind: str
    legacy_table: str
    legacy_query: str
    entity_table: str
    entity_key_column: str
    entity_id_column: str
    entity_columns: tuple[str, ...]
    member_table: str
    member_entity_column: str


@dataclass(frozen=True, slots=True)
class PlannedCatalog:
    kind: str
    catalog_key: str
    name: str
    description: str
    source_type: str
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PlannedEntity:
    kind: str
    resolved_key: str
    fingerprint_sha1: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PlannedMembership:
    kind: str
    catalog_key: str
    resolved_key: str
    ordinal: int
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PlannedAlias:
    kind: str
    legacy_set_key: str
    legacy_external_id: str
    resolved_key: str
    fingerprint_sha1: str
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class InventoryV2Plan:
    catalogs: tuple[PlannedCatalog, ...]
    entities: tuple[PlannedEntity, ...]
    memberships: tuple[PlannedMembership, ...]
    aliases: tuple[PlannedAlias, ...]
    stats: dict[str, dict[str, int]]


@dataclass(frozen=True, slots=True)
class InventoryV2RebuildSummary:
    catalogs_written: int
    entities_written: int
    memberships_written: int
    aliases_written: int
    stats: dict[str, dict[str, int]]


def _normalized_profile_variant(row: Mapping[str, Any]) -> str:
    return _slugify(str(row.get("radio_profile") or "default"))


def _metadata_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    metadata = row.get("metadata")
    return dict(metadata) if isinstance(metadata, Mapping) else {}


def _explicit_profile_key(row: Mapping[str, Any], field_name: str) -> str | None:
    direct_value = row.get(field_name)
    if direct_value is not None and str(direct_value).strip():
        return str(direct_value).strip()
    metadata_value = _metadata_dict(row).get(field_name)
    if metadata_value is None:
        return None
    normalized = str(metadata_value).strip()
    return normalized or None


def _is_geo_russia_row(row: Mapping[str, Any]) -> bool:
    metadata = _metadata_dict(row)
    return "geo-russia" in {
        str(row.get("set_key") or ""),
        str(row.get("catalog_key") or ""),
        str(metadata.get("catalog_key") or ""),
        str(metadata.get("collection_key") or ""),
    }


def _ground_profile_key(kind: str, row: Mapping[str, Any]) -> str:
    explicit = _explicit_profile_key(row, "ground_terminal_profile_key")
    if explicit is not None:
        return explicit
    if kind == "gateways":
        if str(row.get("external_id") or "").startswith("geo-hub-"):
            return "gateway_ground_geo_hub_v1"
        return f"gateway_ground_{_normalized_profile_variant(row)}_v1"
    if kind == "vsats":
        return f"vsat_ground_{_normalized_profile_variant(row)}_v1"
    if kind == "targets":
        return "target_ground_default_v1"
    if kind == "theoretical_subscribers":
        return THEORETICAL_SUBSCRIBER_GROUND_PROFILE_KEY
    raise ValueError(f"Unsupported ground profile kind: {kind}")


def _relay_profile_key(kind: str, row: Mapping[str, Any], scope: str) -> str:
    field_name = "user_beam_profile_key" if scope == "user" else "feeder_beam_profile_key"
    explicit = _explicit_profile_key(row, field_name)
    if explicit is not None:
        return explicit
    if kind == "satellites" and _is_geo_russia_row(row):
        return "satellite_user_geo_highpower_v1" if scope == "user" else "satellite_feeder_highcap_v1"
    prefix = RELAY_PROFILE_KEY_PREFIX.get(kind)
    if prefix is None:
        raise ValueError(f"Unsupported relay profile kind: {kind}")
    return f"{prefix}_{scope}_{_normalized_profile_variant(row)}_v1"


def _ground_station_kind(kind: str) -> str:
    return {
        "gateways": "gateway",
        "vsats": "vsat",
        "theoretical_subscribers": "theoretical_subscriber",
        "targets": "target",
    }[kind]


def _ground_profile_seed(profile_key: str, station_kind: str, *, legacy_radio_profile: str | None) -> dict[str, Any]:
    return {
        "profile_key": str(profile_key),
        "station_kind": str(station_kind),
        "params": dict(GROUND_PROFILE_DEFAULTS[station_kind]),
        "metadata": {
            "seed_version": PROFILE_SEED_VERSION,
            "seed_source": "inventory_v2_rebuild",
            "station_kind": station_kind,
            "legacy_radio_profile": None if legacy_radio_profile is None else str(legacy_radio_profile),
        },
    }


def _curated_ground_profile_seed(profile_key: str, station_kind: str, variant: str) -> dict[str, Any]:
    return {
        "profile_key": str(profile_key),
        "station_kind": str(station_kind),
        "params": dict(CURATED_GROUND_PROFILE_VARIANTS[station_kind][variant]),
        "metadata": {
            "seed_version": PROFILE_SEED_VERSION,
            "seed_source": "inventory_v2_rebuild_curated",
            "station_kind": station_kind,
            "variant": variant,
        },
    }


def _relay_profile_seed(
    profile_key: str,
    *,
    kind: str,
    scope: str,
    legacy_radio_profile: str | None,
) -> dict[str, Any]:
    return {
        "profile_key": str(profile_key),
        "relay_mode": "transparent_relay",
        "params": dict(RELAY_PROFILE_DEFAULTS[(kind, scope)]),
        "metadata": {
            "seed_version": PROFILE_SEED_VERSION,
            "seed_source": "inventory_v2_rebuild",
            "relay_kind": kind,
            "profile_scope": scope,
            "legacy_radio_profile": None if legacy_radio_profile is None else str(legacy_radio_profile),
        },
    }


def _curated_relay_profile_seed(profile_key: str, *, kind: str, scope: str, variant: str) -> dict[str, Any]:
    return {
        "profile_key": str(profile_key),
        "relay_mode": "transparent_relay",
        "params": dict(CURATED_RELAY_PROFILE_VARIANTS[(kind, scope)][variant]),
        "metadata": {
            "seed_version": PROFILE_SEED_VERSION,
            "seed_source": "inventory_v2_rebuild_curated",
            "relay_kind": kind,
            "profile_scope": scope,
            "variant": variant,
        },
    }


def _profile_seed_rows_from_legacy(
    legacy_rows_by_kind: Mapping[str, Sequence[Mapping[str, Any]]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    ground_profiles: dict[str, dict[str, Any]] = {}
    relay_profiles: dict[str, dict[str, Any]] = {}

    ground_profiles.setdefault(
        "gateway_ground_default_v1",
        _ground_profile_seed(
            "gateway_ground_default_v1",
            "gateway",
            legacy_radio_profile="default",
        ),
    )
    ground_profiles.setdefault(
        "vsat_ground_default_v1",
        _ground_profile_seed(
            "vsat_ground_default_v1",
            "vsat",
            legacy_radio_profile="default",
        ),
    )
    ground_profiles.setdefault(
        "target_ground_default_v1",
        _ground_profile_seed(
            "target_ground_default_v1",
            "target",
            legacy_radio_profile="default",
        ),
    )

    for kind in ("gateways", "vsats", "targets"):
        station_kind = _ground_station_kind(kind)
        for row in legacy_rows_by_kind.get(kind, ()):
            profile_key = _ground_profile_key(kind, row)
            variant = _normalized_profile_variant(row)
            if variant in CURATED_GROUND_PROFILE_VARIANTS.get(station_kind, {}):
                seed_payload = _curated_ground_profile_seed(profile_key, station_kind, variant)
            else:
                seed_payload = _ground_profile_seed(
                    profile_key,
                    station_kind,
                    legacy_radio_profile=str(row.get("radio_profile") or ""),
                )
            ground_profiles.setdefault(
                profile_key,
                seed_payload,
            )

    ground_profiles.setdefault(
        THEORETICAL_SUBSCRIBER_GROUND_PROFILE_KEY,
        _ground_profile_seed(
            THEORETICAL_SUBSCRIBER_GROUND_PROFILE_KEY,
            "theoretical_subscriber",
            legacy_radio_profile="default",
        ),
    )
    for station_kind, variants in CURATED_GROUND_PROFILE_VARIANTS.items():
        for variant in variants:
            profile_key = f"{station_kind}_ground_{variant}_v1"
            ground_profiles.setdefault(
                profile_key,
                _curated_ground_profile_seed(profile_key, station_kind, variant),
            )

    for kind in ("satellites", "haps"):
        for scope in ("user", "feeder"):
            prefix = RELAY_PROFILE_KEY_PREFIX[kind]
            default_profile_key = f"{prefix}_{scope}_default_v1"
            relay_profiles.setdefault(
                default_profile_key,
                _relay_profile_seed(
                    default_profile_key,
                    kind=kind,
                    scope=scope,
                    legacy_radio_profile="default",
                ),
            )
        for row in legacy_rows_by_kind.get(kind, ()):
            legacy_radio_profile = str(row.get("radio_profile") or "")
            for scope in ("user", "feeder"):
                profile_key = _relay_profile_key(kind, row, scope)
                variant = _normalized_profile_variant(row)
                if variant in CURATED_RELAY_PROFILE_VARIANTS.get((kind, scope), {}):
                    seed_payload = _curated_relay_profile_seed(
                        profile_key,
                        kind=kind,
                        scope=scope,
                        variant=variant,
                    )
                else:
                    seed_payload = _relay_profile_seed(
                        profile_key,
                        kind=kind,
                        scope=scope,
                        legacy_radio_profile=legacy_radio_profile,
                    )
                relay_profiles.setdefault(
                    profile_key,
                    seed_payload,
                )
        for scope in ("user", "feeder"):
            for variant in CURATED_RELAY_PROFILE_VARIANTS.get((kind, scope), {}):
                prefix = RELAY_PROFILE_KEY_PREFIX[kind]
                profile_key = f"{prefix}_{scope}_{variant}_v1"
                relay_profiles.setdefault(
                    profile_key,
                    _curated_relay_profile_seed(
                        profile_key,
                        kind=kind,
                        scope=scope,
                        variant=variant,
                    ),
                )

    return ground_profiles, relay_profiles


KIND_CONFIGS: dict[str, InventoryKindConfig] = {
    "gateways": InventoryKindConfig(
        kind="gateways",
        legacy_table="gateway_node",
        legacy_query=f"""
            SELECT
                set_key,
                external_id,
                name,
                lat,
                lon,
                site_alt_m,
                antenna_height_agl_m,
                COALESCE(radio_profile, 'default') AS radio_profile,
                metadata->>'ground_terminal_profile_key' AS ground_terminal_profile_key,
                COALESCE(role, 'gateway') AS role,
                COALESCE(limits, '{{}}'::jsonb) AS limits,
                COALESCE(metadata, '{{}}'::jsonb) AS metadata,
                ordinal
            FROM {INVENTORY_SCHEMA}.gateway_node
            ORDER BY set_key, ordinal, external_id
        """,
        entity_table="gateway",
        entity_key_column="gateway_key",
        entity_id_column="gateway_id",
        entity_columns=(
            "name",
            "lat",
            "lon",
            "site_alt_m",
            "antenna_height_agl_m",
            "radio_profile",
            "ground_terminal_profile_key",
            "role",
            "limits",
            "metadata",
        ),
        member_table="gateway_catalog_member",
        member_entity_column="gateway_id",
    ),
    "targets": InventoryKindConfig(
        kind="targets",
        legacy_table="target_hub",
        legacy_query=f"""
            SELECT
                set_key,
                external_id,
                name,
                lat,
                lon,
                frequency,
                priority,
                site_alt_m,
                antenna_height_agl_m,
                metadata->>'ground_terminal_profile_key' AS ground_terminal_profile_key,
                COALESCE(metadata, '{{}}'::jsonb) AS metadata,
                ordinal
            FROM {INVENTORY_SCHEMA}.target_hub
            ORDER BY set_key, ordinal, external_id
        """,
        entity_table="target",
        entity_key_column="target_key",
        entity_id_column="target_id",
        entity_columns=(
            "name",
            "lat",
            "lon",
            "frequency",
            "priority",
            "site_alt_m",
            "antenna_height_agl_m",
            "ground_terminal_profile_key",
            "metadata",
        ),
        member_table="target_catalog_member",
        member_entity_column="target_id",
    ),
    "satellites": InventoryKindConfig(
        kind="satellites",
        legacy_table="satellite_node",
        legacy_query=f"""
            SELECT
                set_key,
                external_id,
                name,
                tle_line1,
                tle_line2,
                COALESCE(radio_profile, 'default') AS radio_profile,
                metadata->>'user_beam_profile_key' AS user_beam_profile_key,
                metadata->>'feeder_beam_profile_key' AS feeder_beam_profile_key,
                connection_min,
                beam_layout_mode,
                dynamic_ray_count,
                dynamic_ray_aperture_deg,
                sat_haps_ray_count,
                COALESCE(resource_limits, '{{}}'::jsonb) AS resource_limits,
                COALESCE(allowed_link_types, '[]'::jsonb) AS allowed_link_types,
                COALESCE(metadata, '{{}}'::jsonb) AS metadata,
                ordinal
            FROM {INVENTORY_SCHEMA}.satellite_node
            ORDER BY set_key, ordinal, external_id
        """,
        entity_table="satellite",
        entity_key_column="satellite_key",
        entity_id_column="satellite_id",
        entity_columns=(
            "name",
            "tle_line1",
            "tle_line2",
            "radio_profile",
            "user_beam_profile_key",
            "feeder_beam_profile_key",
            "connection_min",
            "beam_layout_mode",
            "dynamic_ray_count",
            "dynamic_ray_aperture_deg",
            "sat_haps_ray_count",
            "resource_limits",
            "allowed_link_types",
            "metadata",
        ),
        member_table="satellite_catalog_member",
        member_entity_column="satellite_id",
    ),
    "haps": InventoryKindConfig(
        kind="haps",
        legacy_table="haps_node",
        legacy_query=f"""
            SELECT
                set_key,
                external_id,
                name,
                lat,
                lon,
                alt_m,
                COALESCE(radio_profile, 'default') AS radio_profile,
                metadata->>'user_beam_profile_key' AS user_beam_profile_key,
                metadata->>'feeder_beam_profile_key' AS feeder_beam_profile_key,
                connection_min,
                COALESCE(resource_limits, '{{}}'::jsonb) AS resource_limits,
                COALESCE(metadata, '{{}}'::jsonb) AS metadata,
                ordinal
            FROM {INVENTORY_SCHEMA}.haps_node
            ORDER BY set_key, ordinal, external_id
        """,
        entity_table="haps",
        entity_key_column="haps_key",
        entity_id_column="haps_id",
        entity_columns=(
            "name",
            "lat",
            "lon",
            "alt_m",
            "radio_profile",
            "user_beam_profile_key",
            "feeder_beam_profile_key",
            "connection_min",
            "resource_limits",
            "metadata",
        ),
        member_table="haps_catalog_member",
        member_entity_column="haps_id",
    ),
    "vsats": InventoryKindConfig(
        kind="vsats",
        legacy_table="vsat_node",
        legacy_query=f"""
            SELECT
                set_key,
                external_id,
                name,
                lat,
                lon,
                region_code,
                site_alt_m,
                antenna_height_agl_m,
                COALESCE(radio_profile, 'default') AS radio_profile,
                metadata->>'ground_terminal_profile_key' AS ground_terminal_profile_key,
                COALESCE(role, 'vsat') AS role,
                COALESCE(limits, '{{}}'::jsonb) AS limits,
                COALESCE(metadata, '{{}}'::jsonb) AS metadata,
                ordinal
            FROM {INVENTORY_SCHEMA}.vsat_node
            ORDER BY set_key, ordinal, external_id
        """,
        entity_table="vsat",
        entity_key_column="vsat_key",
        entity_id_column="vsat_id",
        entity_columns=(
            "name",
            "lat",
            "lon",
            "region_code",
            "site_alt_m",
            "antenna_height_agl_m",
            "radio_profile",
            "ground_terminal_profile_key",
            "role",
            "limits",
            "metadata",
        ),
        member_table="vsat_catalog_member",
        member_entity_column="vsat_id",
    ),
}


def _require_psycopg() -> object:
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise RuntimeError(
            "Inventory PostgreSQL support requires the 'psycopg' package. "
            "Install dependencies from requirements.txt before rebuilding inventory_v2."
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


def _fetch_rows(cursor: Any, query: str) -> list[dict[str, Any]]:
    cursor.execute(query)
    columns = [str(desc[0]) for desc in cursor.description]
    rows: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        rows.append({columns[index]: row[index] for index in range(len(columns))})
    return rows


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_json_value(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalize_json_value(item) for item in value]
    return value


def _sanitize_metadata(value: Any) -> Any:
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key)
            if key in IGNORED_METADATA_KEYS:
                continue
            sanitized[key] = _sanitize_metadata(raw_value)
        return sanitized
    if isinstance(value, (list, tuple)):
        return [_sanitize_metadata(item) for item in value]
    return value


def _coerce_entity_value(column: str, value: Any) -> Any:
    if value is None:
        if column == "allowed_link_types":
            return []
        if column in {"metadata", "limits", "resource_limits"}:
            return {}
        return None
    if column == "metadata":
        return _sanitize_metadata(value)
    if column == "allowed_link_types":
        return [str(item) for item in list(value)]
    if column in {"limits", "resource_limits"}:
        return _normalize_json_value(value)
    return value


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z]+", "-", str(value).strip().lower()).strip("-")
    return normalized or "item"


def _fingerprint_for_entity(kind: str, external_id: str, payload: Mapping[str, Any]) -> str:
    canonical_payload = {
        "kind": kind,
        "external_id": str(external_id),
        "payload": _normalize_json_value(dict(payload)),
    }
    encoded = json.dumps(canonical_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


def _resolved_key_for_entity(external_id: str, fingerprint_sha1: str, conflicting_external_ids: set[str]) -> str:
    normalized_external_id = str(external_id).strip()
    if normalized_external_id and normalized_external_id not in conflicting_external_ids:
        return normalized_external_id
    return f"{_slugify(normalized_external_id)}-{fingerprint_sha1[:10]}"


def _catalog_sort_key(item: PlannedCatalog) -> tuple[str, str]:
    return (item.kind, item.catalog_key)


def _entity_sort_key(item: PlannedEntity) -> tuple[str, str]:
    return (item.kind, item.resolved_key)


def _membership_sort_key(item: PlannedMembership) -> tuple[str, str, int, str]:
    return (item.kind, item.catalog_key, int(item.ordinal), item.resolved_key)


def _alias_sort_key(item: PlannedAlias) -> tuple[str, str, str]:
    return (item.kind, item.legacy_set_key, item.legacy_external_id)


def _entity_payload_from_legacy_row(config: InventoryKindConfig, row: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for column in config.entity_columns:
        payload[column] = _coerce_entity_value(column, row.get(column))
    if config.kind in {"gateways", "vsats", "targets"}:
        payload["ground_terminal_profile_key"] = _ground_profile_key(config.kind, row)
    if config.kind in {"satellites", "haps"}:
        payload["user_beam_profile_key"] = _relay_profile_key(config.kind, row, "user")
        payload["feeder_beam_profile_key"] = _relay_profile_key(config.kind, row, "feeder")
    if config.kind == "satellites" and _is_geo_russia_row(row):
        resource_limits = dict(payload.get("resource_limits", {}))
        resource_limits["access_model"] = "geo_wide_footprint"
        payload["resource_limits"] = resource_limits
        payload["connection_min"] = 1
        payload["user_beam_profile_key"] = "satellite_user_geo_highpower_v1"
        payload["feeder_beam_profile_key"] = "satellite_feeder_highcap_v1"
    return payload


def plan_inventory_v2_rebuild(legacy_rows_by_kind: Mapping[str, Sequence[Mapping[str, Any]]]) -> InventoryV2Plan:
    planned_catalogs: list[PlannedCatalog] = []
    planned_entities: list[PlannedEntity] = []
    planned_memberships: list[PlannedMembership] = []
    planned_aliases: list[PlannedAlias] = []
    ground_profiles, relay_profiles = _profile_seed_rows_from_legacy(legacy_rows_by_kind)
    stats: dict[str, dict[str, int]] = {
        "ground_terminal_profiles": {"count": len(ground_profiles)},
        "relay_payload_profiles": {"count": len(relay_profiles)},
        "profile_assignment_rules": {"count": len(DEFAULT_PROFILE_ASSIGNMENT_RULES)},
    }

    for kind, config in KIND_CONFIGS.items():
        rows = [dict(row) for row in legacy_rows_by_kind.get(kind, ())]
        catalogs_by_key: dict[str, PlannedCatalog] = {}
        entity_by_fingerprint: dict[str, dict[str, Any]] = {}
        membership_by_catalog_and_entity: dict[tuple[str, str], dict[str, Any]] = {}
        alias_rows: list[dict[str, Any]] = []

        for row in rows:
            legacy_set_key = str(row.get("set_key") or "").strip()
            legacy_external_id = str(row.get("external_id") or "").strip()
            ordinal = int(row.get("ordinal") or 0)
            if not legacy_set_key:
                raise ValueError(f"Legacy inventory row for '{kind}' is missing set_key")
            if not legacy_external_id:
                raise ValueError(f"Legacy inventory row for '{kind}' is missing external_id")

            catalogs_by_key.setdefault(
                legacy_set_key,
                PlannedCatalog(
                    kind=kind,
                    catalog_key=legacy_set_key,
                    name=f"{kind}:{legacy_set_key}",
                    description=f"Migrated legacy selection set '{legacy_set_key}' for '{kind}'",
                    source_type="legacy_set",
                    metadata={
                        "legacy_schema": INVENTORY_SCHEMA,
                        "legacy_table": config.legacy_table,
                    },
                ),
            )

            payload = _entity_payload_from_legacy_row(config, row)
            fingerprint_sha1 = _fingerprint_for_entity(kind, legacy_external_id, payload)
            entity_by_fingerprint.setdefault(
                fingerprint_sha1,
                {
                    "external_id": legacy_external_id,
                    "fingerprint_sha1": fingerprint_sha1,
                    "payload": payload,
                },
            )

            membership_key = (legacy_set_key, fingerprint_sha1)
            current_membership = membership_by_catalog_and_entity.get(membership_key)
            next_membership = {
                "catalog_key": legacy_set_key,
                "fingerprint_sha1": fingerprint_sha1,
                "ordinal": ordinal,
                "metadata": {"legacy_external_id": legacy_external_id},
            }
            if current_membership is None or int(next_membership["ordinal"]) < int(current_membership["ordinal"]):
                membership_by_catalog_and_entity[membership_key] = next_membership

            alias_rows.append(
                {
                    "legacy_set_key": legacy_set_key,
                    "legacy_external_id": legacy_external_id,
                    "fingerprint_sha1": fingerprint_sha1,
                    "metadata": {
                        "legacy_name": str(row.get("name") or legacy_external_id),
                        "legacy_ordinal": ordinal,
                        "legacy_table": config.legacy_table,
                    },
                }
            )

        conflicting_external_ids = {
            external_id
            for external_id, count in Counter(
                str(entity["external_id"]) for entity in entity_by_fingerprint.values()
            ).items()
            if count > 1
        }
        resolved_key_by_fingerprint: dict[str, str] = {}

        for fingerprint_sha1, entity in entity_by_fingerprint.items():
            resolved_key = _resolved_key_for_entity(
                str(entity["external_id"]),
                fingerprint_sha1,
                conflicting_external_ids,
            )
            resolved_key_by_fingerprint[fingerprint_sha1] = resolved_key
            planned_entities.append(
                PlannedEntity(
                    kind=kind,
                    resolved_key=resolved_key,
                    fingerprint_sha1=fingerprint_sha1,
                    payload=dict(entity["payload"]),
                )
            )

        planned_catalogs.extend(catalogs_by_key.values())
        for membership in membership_by_catalog_and_entity.values():
            planned_memberships.append(
                PlannedMembership(
                    kind=kind,
                    catalog_key=str(membership["catalog_key"]),
                    resolved_key=resolved_key_by_fingerprint[str(membership["fingerprint_sha1"])],
                    ordinal=int(membership["ordinal"]),
                    metadata=dict(membership["metadata"]),
                )
            )
        for alias in alias_rows:
            planned_aliases.append(
                PlannedAlias(
                    kind=kind,
                    legacy_set_key=str(alias["legacy_set_key"]),
                    legacy_external_id=str(alias["legacy_external_id"]),
                    resolved_key=resolved_key_by_fingerprint[str(alias["fingerprint_sha1"])],
                    fingerprint_sha1=str(alias["fingerprint_sha1"]),
                    metadata=dict(alias["metadata"]),
                )
            )

        stats[kind] = {
            "legacy_rows": len(rows),
            "catalogs": len(catalogs_by_key),
            "unique_entities": len(entity_by_fingerprint),
            "memberships": len(membership_by_catalog_and_entity),
            "aliases": len(alias_rows),
        }

    return InventoryV2Plan(
        catalogs=tuple(sorted(planned_catalogs, key=_catalog_sort_key)),
        entities=tuple(sorted(planned_entities, key=_entity_sort_key)),
        memberships=tuple(sorted(planned_memberships, key=_membership_sort_key)),
        aliases=tuple(sorted(planned_aliases, key=_alias_sort_key)),
        stats=stats,
    )


def ensure_schema_v2(postgres: PostgresSettings | None = None) -> None:
    postgres = postgres or load_postgres_settings()
    from .migrations import apply_migrations

    apply_migrations(postgres, target_version="0004_explicit_resource_limit_runtime_fields")


def _reset_inventory_v2(cursor: Any) -> None:
    cursor.execute(
        f"""
        TRUNCATE TABLE
            {INVENTORY_V2_SCHEMA}.legacy_inventory_alias,
            {INVENTORY_V2_SCHEMA}.gateway_catalog_member,
            {INVENTORY_V2_SCHEMA}.target_catalog_member,
            {INVENTORY_V2_SCHEMA}.satellite_catalog_member,
            {INVENTORY_V2_SCHEMA}.haps_catalog_member,
            {INVENTORY_V2_SCHEMA}.vsat_catalog_member,
            {INVENTORY_V2_SCHEMA}.theoretical_subscriber_catalog_member,
            {INVENTORY_V2_SCHEMA}.selection_catalog,
            {INVENTORY_V2_SCHEMA}.ground_terminal_profile,
            {INVENTORY_V2_SCHEMA}.relay_payload_profile,
            {INVENTORY_V2_SCHEMA}.profile_assignment_rule,
            {INVENTORY_V2_SCHEMA}.gateway,
            {INVENTORY_V2_SCHEMA}.target,
            {INVENTORY_V2_SCHEMA}.satellite,
            {INVENTORY_V2_SCHEMA}.haps,
            {INVENTORY_V2_SCHEMA}.vsat,
            {INVENTORY_V2_SCHEMA}.theoretical_subscriber
        RESTART IDENTITY CASCADE
        """
    )


def _legacy_rows_by_kind(cursor: Any) -> dict[str, list[dict[str, Any]]]:
    rows_by_kind: dict[str, list[dict[str, Any]]] = {}
    for kind, config in KIND_CONFIGS.items():
        rows_by_kind[kind] = _fetch_rows(cursor, config.legacy_query)
    return rows_by_kind


def _db_value(column: str, value: Any, Jsonb: Any) -> Any:
    if column in JSON_COLUMNS:
        if column == "allowed_link_types":
            return Jsonb(list(value or []))
        return Jsonb(dict(value or {}))
    return value


def _upsert_profile_tables(
    cursor: Any,
    Jsonb: Any,
    legacy_rows_by_kind: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, dict[str, int]]:
    ground_profiles, relay_profiles = _profile_seed_rows_from_legacy(legacy_rows_by_kind)

    for profile in ground_profiles.values():
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
                str(profile["profile_key"]),
                str(profile["station_kind"]),
                Jsonb(dict(profile["params"])),
                Jsonb(dict(profile["metadata"])),
            ),
        )

    for profile in relay_profiles.values():
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
                str(profile["profile_key"]),
                str(profile["relay_mode"]),
                Jsonb(dict(profile["params"])),
                Jsonb(dict(profile["metadata"])),
            ),
        )

    return {
        "ground_terminal_profiles": {"count": len(ground_profiles)},
        "relay_payload_profiles": {"count": len(relay_profiles)},
    }


def _upsert_default_profile_assignment_rules(
    cursor: Any,
    Jsonb: Any,
) -> dict[str, dict[str, int]]:
    for rule in DEFAULT_PROFILE_ASSIGNMENT_RULES:
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
            ON CONFLICT (rule_key) DO UPDATE SET
                name = EXCLUDED.name,
                kind = EXCLUDED.kind,
                catalog_key = EXCLUDED.catalog_key,
                query = EXCLUDED.query,
                bbox = EXCLUDED.bbox,
                region_codes = EXCLUDED.region_codes,
                subject_codes = EXCLUDED.subject_codes,
                federal_districts = EXCLUDED.federal_districts,
                grid_cell_ids = EXCLUDED.grid_cell_ids,
                is_active_filter = EXCLUDED.is_active_filter,
                ground_terminal_profile_key = EXCLUDED.ground_terminal_profile_key,
                user_beam_profile_key = EXCLUDED.user_beam_profile_key,
                feeder_beam_profile_key = EXCLUDED.feeder_beam_profile_key,
                priority = EXCLUDED.priority,
                is_active = EXCLUDED.is_active,
                metadata = EXCLUDED.metadata,
                updated_at = now()
            """,
            (
                str(rule["rule_key"]),
                str(rule["name"]),
                str(rule["kind"]),
                rule.get("catalog_key"),
                rule.get("query"),
                Jsonb(dict(rule.get("bbox", {}))),
                Jsonb(list(rule.get("region_codes", []))),
                Jsonb(list(rule.get("subject_codes", []))),
                Jsonb(list(rule.get("federal_districts", []))),
                Jsonb(list(rule.get("grid_cell_ids", []))),
                rule.get("is_active_filter"),
                rule.get("ground_terminal_profile_key"),
                rule.get("user_beam_profile_key"),
                rule.get("feeder_beam_profile_key"),
                int(rule.get("priority", 100)),
                True,
                Jsonb(
                    {
                        "seed_version": PROFILE_SEED_VERSION,
                        "seed_source": "inventory_v2_rebuild",
                        "rule_scope": "kind_default",
                    }
                ),
            ),
        )
    return {"profile_assignment_rules": {"count": len(DEFAULT_PROFILE_ASSIGNMENT_RULES)}}


def _rebuild_theoretical_subscribers(
    cursor: Any,
    Jsonb: Any,
) -> dict[str, int]:
    seed_rows = load_theoretical_subscriber_seed_rows(THEORETICAL_SUBSCRIBER_ASSET_PATH)
    cursor.execute(
        f"""
        INSERT INTO {INVENTORY_V2_SCHEMA}.selection_catalog (
            kind,
            catalog_key,
            name,
            description,
            source_type,
            metadata
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (kind, catalog_key) DO UPDATE SET
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            source_type = EXCLUDED.source_type,
            metadata = EXCLUDED.metadata
        RETURNING catalog_id
        """,
        (
            "theoretical_subscribers",
            THEORETICAL_SUBSCRIBER_CATALOG_KEY,
            "Theoretical subscribers RU grid",
            "Static Russia theoretical subscriber grid seed",
            "seed_asset",
            Jsonb(
                {
                    "seed_version": THEORETICAL_SUBSCRIBER_SEED_VERSION,
                    "asset_path": str(THEORETICAL_SUBSCRIBER_ASSET_PATH),
                }
            ),
        ),
    )
    catalog_id = int(cursor.fetchone()[0])

    entity_count = 0
    membership_count = 0
    for ordinal, row in enumerate(seed_rows):
        payload = {column: row.get(column) for column in THEORETICAL_SUBSCRIBER_COLUMNS}
        if not payload.get("ground_terminal_profile_key"):
            payload["ground_terminal_profile_key"] = THEORETICAL_SUBSCRIBER_GROUND_PROFILE_KEY
        fingerprint_sha1 = _fingerprint_for_entity(
            "theoretical_subscribers",
            str(row["subscriber_key"]),
            payload,
        )
        insert_columns = ("subscriber_key", "fingerprint_sha1", *THEORETICAL_SUBSCRIBER_COLUMNS)
        update_columns = [f"{column} = EXCLUDED.{column}" for column in insert_columns]
        values = [str(row["subscriber_key"]), fingerprint_sha1]
        for column in THEORETICAL_SUBSCRIBER_COLUMNS:
            values.append(_db_value(column, payload.get(column), Jsonb))
        cursor.execute(
            f"""
            INSERT INTO {INVENTORY_V2_SCHEMA}.theoretical_subscriber (
                {", ".join(insert_columns)}
            ) VALUES ({", ".join(["%s"] * len(insert_columns))})
            ON CONFLICT (subscriber_key) DO UPDATE SET
                {", ".join(update_columns)}
            RETURNING theoretical_subscriber_id
            """,
            tuple(values),
        )
        theoretical_subscriber_id = int(cursor.fetchone()[0])
        entity_count += 1
        cursor.execute(
            f"""
            INSERT INTO {INVENTORY_V2_SCHEMA}.theoretical_subscriber_catalog_member (
                catalog_id,
                theoretical_subscriber_id,
                ordinal,
                metadata
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (catalog_id, theoretical_subscriber_id) DO UPDATE SET
                ordinal = EXCLUDED.ordinal,
                metadata = EXCLUDED.metadata
            """,
            (
                catalog_id,
                theoretical_subscriber_id,
                int(ordinal),
                Jsonb(
                    {
                        "seed_version": str(row["seed_version"]),
                        "subject_code": int(row["subject_code"]),
                    }
                ),
            ),
        )
        membership_count += 1

    return {
        "legacy_rows": 0,
        "catalogs": 1,
        "unique_entities": entity_count,
        "memberships": membership_count,
        "aliases": 0,
    }


def rebuild_inventory_v2_from_legacy(
    postgres: PostgresSettings | None = None,
    *,
    reset: bool = True,
) -> InventoryV2RebuildSummary:
    postgres = postgres or load_postgres_settings()
    psycopg, Jsonb = _require_psycopg()
    ensure_schema_v2(postgres)

    with psycopg.connect(**_connection_kwargs(postgres)) as connection:
        with connection.cursor() as cursor:
            legacy_rows = _legacy_rows_by_kind(cursor)
            plan = plan_inventory_v2_rebuild(legacy_rows)
            theoretical_stats = {
                "legacy_rows": 0,
                "catalogs": 0,
                "unique_entities": 0,
                "memberships": 0,
                "aliases": 0,
            }
            if reset:
                _reset_inventory_v2(cursor)

            profile_stats = _upsert_profile_tables(
                cursor,
                Jsonb,
                legacy_rows,
            )
            plan.stats.update(profile_stats)
            plan.stats.update(_upsert_default_profile_assignment_rules(cursor, Jsonb))

            catalog_id_by_key: dict[tuple[str, str], int] = {}
            for catalog in plan.catalogs:
                cursor.execute(
                    f"""
                    INSERT INTO {INVENTORY_V2_SCHEMA}.selection_catalog (
                        kind,
                        catalog_key,
                        name,
                        description,
                        source_type,
                        metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (kind, catalog_key) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        source_type = EXCLUDED.source_type,
                        metadata = EXCLUDED.metadata
                    RETURNING catalog_id
                    """,
                    (
                        catalog.kind,
                        catalog.catalog_key,
                        catalog.name,
                        catalog.description,
                        catalog.source_type,
                        Jsonb(dict(catalog.metadata)),
                    ),
                )
                catalog_id_by_key[(catalog.kind, catalog.catalog_key)] = int(cursor.fetchone()[0])

            entity_id_by_key: dict[tuple[str, str], int] = {}
            for entity in plan.entities:
                config = KIND_CONFIGS[entity.kind]
                insert_columns = (config.entity_key_column, "fingerprint_sha1", *config.entity_columns)
                update_columns = [f"{column} = EXCLUDED.{column}" for column in insert_columns]
                values = [entity.resolved_key, entity.fingerprint_sha1]
                for column in config.entity_columns:
                    values.append(_db_value(column, entity.payload.get(column), Jsonb))
                cursor.execute(
                    f"""
                    INSERT INTO {INVENTORY_V2_SCHEMA}.{config.entity_table} (
                        {", ".join(insert_columns)}
                    ) VALUES ({", ".join(["%s"] * len(insert_columns))})
                    ON CONFLICT (fingerprint_sha1) DO UPDATE SET
                        {", ".join(update_columns)}
                    RETURNING {config.entity_id_column}
                    """,
                    tuple(values),
                )
                entity_id_by_key[(entity.kind, entity.resolved_key)] = int(cursor.fetchone()[0])

            for membership in plan.memberships:
                config = KIND_CONFIGS[membership.kind]
                cursor.execute(
                    f"""
                    INSERT INTO {INVENTORY_V2_SCHEMA}.{config.member_table} (
                        catalog_id,
                        {config.member_entity_column},
                        ordinal,
                        metadata
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (catalog_id, {config.member_entity_column}) DO UPDATE SET
                        ordinal = EXCLUDED.ordinal,
                        metadata = EXCLUDED.metadata
                    """,
                    (
                        catalog_id_by_key[(membership.kind, membership.catalog_key)],
                        entity_id_by_key[(membership.kind, membership.resolved_key)],
                        membership.ordinal,
                        Jsonb(dict(membership.metadata)),
                    ),
                )

            for alias in plan.aliases:
                cursor.execute(
                    f"""
                    INSERT INTO {INVENTORY_V2_SCHEMA}.legacy_inventory_alias (
                        kind,
                        legacy_set_key,
                        legacy_external_id,
                        resolved_key,
                        fingerprint_sha1,
                        metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (kind, legacy_set_key, legacy_external_id) DO UPDATE SET
                        resolved_key = EXCLUDED.resolved_key,
                        fingerprint_sha1 = EXCLUDED.fingerprint_sha1,
                        metadata = EXCLUDED.metadata
                    """,
                    (
                        alias.kind,
                        alias.legacy_set_key,
                        alias.legacy_external_id,
                        alias.resolved_key,
                        alias.fingerprint_sha1,
                        Jsonb(dict(alias.metadata)),
                    ),
                )

            theoretical_stats = _rebuild_theoretical_subscribers(
                cursor,
                Jsonb,
            )
            plan.stats["theoretical_subscribers"] = theoretical_stats

        connection.commit()

    return InventoryV2RebuildSummary(
        catalogs_written=len(plan.catalogs) + int(theoretical_stats["catalogs"]),
        entities_written=len(plan.entities) + int(theoretical_stats["unique_entities"]),
        memberships_written=len(plan.memberships) + int(theoretical_stats["memberships"]),
        aliases_written=len(plan.aliases) + int(theoretical_stats["aliases"]),
        stats=plan.stats,
    )
