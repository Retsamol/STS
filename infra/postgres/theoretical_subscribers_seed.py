from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
THEORETICAL_SUBSCRIBER_SEED_VERSION = "ru_v1"
THEORETICAL_SUBSCRIBER_CATALOG_KEY = "ru-grid-v1"
THEORETICAL_SUBSCRIBER_ASSET_PATH = PROJECT_ROOT / "data" / "theoretical_subscribers_ru_v1.csv"
LEGACY_GRID_SOURCE_PATH = PROJECT_ROOT / "configs_old" / "vsat_payload.db"

SUBJECT_CODE_TO_FEDERAL_DISTRICT: dict[int, str] = {
    0: "Арктика",
    1: "Сибирский федеральный округ",
    3: "Южный федеральный округ",
    4: "Сибирский федеральный округ",
    5: "Дальневосточный федеральный округ",
    7: "Северо-Кавказский федеральный округ",
    8: "Дальневосточный федеральный округ",
    10: "Дальневосточный федеральный округ",
    11: "Северо-Западный федеральный округ",
    12: "Южный федеральный округ",
    14: "Центральный федеральный округ",
    15: "Центральный федеральный округ",
    17: "Центральный федеральный округ",
    18: "Южный федеральный округ",
    19: "Северо-Западный федеральный округ",
    20: "Центральный федеральный округ",
    22: "Приволжский федеральный округ",
    24: "Центральный федеральный округ",
    25: "Сибирский федеральный округ",
    26: "Северо-Кавказский федеральный округ",
    27: "Северо-Западный федеральный округ",
    28: "Центральный федеральный округ",
    29: "Центральный федеральный округ",
    30: "Дальневосточный федеральный округ",
    32: "Сибирский федеральный округ",
    33: "Приволжский федеральный округ",
    34: "Центральный федеральный округ",
    35: "Южный федеральный округ",
    36: "Приволжский федеральный округ",
    37: "Уральский федеральный округ",
    38: "Центральный федеральный округ",
    41: "Северо-Западный федеральный округ",
    42: "Центральный федеральный округ",
    44: "Дальневосточный федеральный округ",
    46: "Центральный федеральный округ",
    47: "Северо-Западный федеральный округ",
    49: "Северо-Западный федеральный округ",
    50: "Сибирский федеральный округ",
    52: "Сибирский федеральный округ",
    53: "Приволжский федеральный округ",
    54: "Центральный федеральный округ",
    56: "Приволжский федеральный округ",
    57: "Приволжский федеральный округ",
    58: "Северо-Западный федеральный округ",
    60: "Южный федеральный округ",
    61: "Центральный федеральный округ",
    63: "Приволжский федеральный округ",
    64: "Дальневосточный федеральный округ",
    65: "Уральский федеральный округ",
    66: "Центральный федеральный округ",
    68: "Центральный федеральный округ",
    69: "Сибирский федеральный округ",
    70: "Центральный федеральный округ",
    71: "Уральский федеральный округ",
    73: "Приволжский федеральный округ",
    75: "Уральский федеральный округ",
    76: "Дальневосточный федеральный округ",
    77: "Дальневосточный федеральный округ",
    78: "Центральный федеральный округ",
    79: "Южный федеральный округ",
    80: "Приволжский федеральный округ",
    81: "Дальневосточный федеральный округ",
    82: "Северо-Кавказский федеральный округ",
    83: "Северо-Кавказский федеральный округ",
    84: "Сибирский федеральный округ",
    85: "Южный федеральный округ",
    86: "Северо-Западный федеральный округ",
    87: "Северо-Западный федеральный округ",
    88: "Приволжский федеральный округ",
    89: "Приволжский федеральный округ",
    90: "Северо-Кавказский федеральный округ",
    91: "Северо-Кавказский федеральный округ",
    92: "Приволжский федеральный округ",
    93: "Сибирский федеральный округ",
    94: "Приволжский федеральный округ",
    95: "Сибирский федеральный округ",
    96: "Северо-Кавказский федеральный округ",
    97: "Приволжский федеральный округ",
    98: "Дальневосточный федеральный округ",
    99: "Дальневосточный федеральный округ",
    111: "Северо-Западный федеральный округ",
    71100: "Уральский федеральный округ",
    71140: "Уральский федеральный округ",
}


def _grid_cell_id(lat: float, lon: float) -> str:
    return f"ru-{lat:+06.2f}-{lon:+07.2f}"


def generate_theoretical_subscriber_seed_rows(
    legacy_grid_path: Path | None = None,
) -> list[dict[str, Any]]:
    source_path = (legacy_grid_path or LEGACY_GRID_SOURCE_PATH).resolve()
    rows: list[dict[str, Any]] = []
    with sqlite3.connect(source_path) as connection:
        cursor = connection.cursor()
        region_rows = {
            int(code): {"subject_name": str(name), "timezone": int(timezone)}
            for name, code, timezone in cursor.execute(
                "SELECT region, code, timezone FROM vsat_regions"
            )
        }
        for legacy_id, lat, lon, _alt, region_code in cursor.execute(
            "SELECT vsats_id, lat, lon, alt, region FROM vsat ORDER BY vsats_id"
        ):
            code = int(region_code)
            region_info = region_rows.get(code, {})
            subject_name = str(region_info.get("subject_name", f"Region {code}"))
            rows.append(
                {
                    "subscriber_key": f"theoretical-subscriber-{int(legacy_id)}",
                    "name": f"Theoretical subscriber {int(legacy_id)}",
                    "lat": float(lat),
                    "lon": float(lon),
                    "site_alt_m": "",
                    "subject_code": code,
                    "subject_name": subject_name,
                    "federal_district": SUBJECT_CODE_TO_FEDERAL_DISTRICT.get(code, "Неизвестный федеральный округ"),
                    "grid_cell_id": _grid_cell_id(float(lat), float(lon)),
                    "seed_version": THEORETICAL_SUBSCRIBER_SEED_VERSION,
                    "is_active": "true",
                    "metadata": json.dumps(
                        {
                            "legacy_grid_id": int(legacy_id),
                            "legacy_region_code": code,
                            "legacy_region_name": subject_name,
                            "timezone": int(region_info.get("timezone", 0)),
                            "source": "ru_grid_seed",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                }
            )
    return rows


def write_theoretical_subscriber_seed_csv(
    output_path: Path | None = None,
    *,
    legacy_grid_path: Path | None = None,
) -> Path:
    destination = (output_path or THEORETICAL_SUBSCRIBER_ASSET_PATH).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "subscriber_key",
        "name",
        "lat",
        "lon",
        "site_alt_m",
        "subject_code",
        "subject_name",
        "federal_district",
        "grid_cell_id",
        "seed_version",
        "is_active",
        "metadata",
    ]
    rows = generate_theoretical_subscriber_seed_rows(legacy_grid_path)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return destination


def load_theoretical_subscriber_seed_rows(
    asset_path: Path | None = None,
) -> list[dict[str, Any]]:
    source_path = (asset_path or THEORETICAL_SUBSCRIBER_ASSET_PATH).resolve()
    rows: list[dict[str, Any]] = []
    with source_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            metadata_raw = raw_row.get("metadata") or "{}"
            try:
                metadata = json.loads(metadata_raw)
            except json.JSONDecodeError:
                metadata = {"raw_metadata": metadata_raw}
            site_alt_raw = (raw_row.get("site_alt_m") or "").strip()
            rows.append(
                {
                    "subscriber_key": str(raw_row["subscriber_key"]).strip(),
                    "name": str(raw_row["name"]).strip(),
                    "lat": float(raw_row["lat"]),
                    "lon": float(raw_row["lon"]),
                    "site_alt_m": None if not site_alt_raw else float(site_alt_raw),
                    "subject_code": int(raw_row["subject_code"]),
                    "subject_name": str(raw_row["subject_name"]).strip(),
                    "federal_district": str(raw_row["federal_district"]).strip(),
                    "grid_cell_id": str(raw_row["grid_cell_id"]).strip(),
                    "seed_version": str(raw_row["seed_version"]).strip(),
                    "is_active": str(raw_row.get("is_active", "true")).strip().lower() not in {"0", "false", "no"},
                    "metadata": metadata if isinstance(metadata, dict) else {"metadata": metadata},
                }
            )
    return rows
