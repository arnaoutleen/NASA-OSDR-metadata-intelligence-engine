from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Iterable, Mapping, Sequence

import pandas as pd

CANONICAL_RETRIEVAL_COLUMNS = [
    "osd_id",
    "sample_id",
    "source_name",
    "RR_mission",
    "organism",
    "material_type",
    "measurement_types",
    "technology_types",
    "device_platforms",
    "data_files",
]

COLUMN_ALIASES = {
    "osd": "osd_id",
    "osd_id": "osd_id",
    "study_id": "osd_id",
    "sample_name": "sample_id",
    "sample_id": "sample_id",
    "source_name": "source_name",
    "source": "source_name",
    "rr_mission": "RR_mission",
    "mission": "RR_mission",
    "organism": "organism",
    "material_type": "material_type",
    "measurement_types": "measurement_types",
    "technology_types": "technology_types",
    "device_platforms": "device_platforms",
    "data_files": "data_files",
}


def _join_if_sequence(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return " | ".join(str(x) for x in value if x not in (None, ""))
    return str(value)



def record_to_canonical_row(record: Any, mission: str | None = None) -> Dict[str, Any]:
    if is_dataclass(record):
        payload = asdict(record)
    elif isinstance(record, Mapping):
        payload = dict(record)
    else:
        payload = dict(vars(record))

    row = {
        "osd_id": payload.get("osd") or payload.get("osd_id") or "",
        "sample_id": payload.get("sample_name") or payload.get("sample_id") or "",
        "source_name": payload.get("source_name") or "",
        "RR_mission": mission or payload.get("RR_mission") or payload.get("mission") or "",
        "organism": payload.get("organism") or "",
        "material_type": payload.get("material_type") or "",
        "measurement_types": _join_if_sequence(payload.get("measurement_types")),
        "technology_types": _join_if_sequence(payload.get("technology_types")),
        "device_platforms": _join_if_sequence(payload.get("device_platforms")),
        "data_files": _join_if_sequence(payload.get("data_files")),
    }
    return row



def records_to_dataframe(records: Iterable[Any], mission: str | None = None) -> pd.DataFrame:
    rows = [record_to_canonical_row(record, mission=mission) for record in records]
    df = pd.DataFrame(rows)
    for col in CANONICAL_RETRIEVAL_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[CANONICAL_RETRIEVAL_COLUMNS].copy()



def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.rename(columns={col: COLUMN_ALIASES.get(col, col) for col in df.columns})
    for col in CANONICAL_RETRIEVAL_COLUMNS:
        if col not in renamed.columns:
            renamed[col] = ""
    return renamed
