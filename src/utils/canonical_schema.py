from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Iterable, Mapping, Sequence

import pandas as pd

BASE_COLUMNS = [
    "osd_id",
    "sample_id",
    "source_name",
    "project",
    "RR_mission",
    "organism",
    "material_type",
    "measurement_types",
    "technology_types",
    "device_platforms",
    "data_files",
    "assay_names",
    "ms_assay_names",
    "extract_names",
]

COLUMN_ALIASES = {
    "osd": "osd_id",
    "osd_id": "osd_id",
    "study_id": "osd_id",
    "sample_name": "sample_id",
    "sample_id": "sample_id",
    "source_name": "source_name",
    "source": "source_name",
    "project": "project",
    "rr_mission": "RR_mission",
    "mission": "RR_mission",
    "organism": "organism",
    "material_type": "material_type",
    "measurement_types": "measurement_types",
    "technology_types": "technology_types",
    "device_platforms": "device_platforms",
    "data_files": "data_files",
    "assay_names": "assay_names",
    "ms_assay_names": "ms_assay_names",
    "extract_names": "extract_names",
}


def _join_if_sequence(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, dict)):
        return " | ".join(str(x) for x in value if x not in (None, ""))
    return str(value)



def _stringify_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)



def record_to_canonical_row(record: Any, project: str | None = None, mission: str | None = None) -> Dict[str, Any]:
    if is_dataclass(record):
        payload = asdict(record)
    elif isinstance(record, Mapping):
        payload = dict(record)
    else:
        payload = dict(vars(record))

    row: Dict[str, Any] = {
        "osd_id": payload.get("osd") or payload.get("osd_id") or "",
        "sample_id": payload.get("sample_name") or payload.get("sample_id") or "",
        "source_name": payload.get("source_name") or "",
        "project": project or payload.get("project") or payload.get("RR_mission") or payload.get("mission") or "",
        "RR_mission": mission or project or payload.get("RR_mission") or payload.get("mission") or "",
        "organism": payload.get("organism") or "",
        "material_type": payload.get("material_type") or "",
        "measurement_types": _join_if_sequence(payload.get("measurement_types")),
        "technology_types": _join_if_sequence(payload.get("technology_types")),
        "device_platforms": _join_if_sequence(payload.get("device_platforms")),
        "data_files": _join_if_sequence(payload.get("data_files")),
        "assay_names": _join_if_sequence(payload.get("assay_names")),
        "ms_assay_names": _join_if_sequence(payload.get("ms_assay_names")),
        "extract_names": _join_if_sequence(payload.get("extract_names")),
    }

    for key, value in payload.items():
        alias = COLUMN_ALIASES.get(key)
        if alias and alias not in row:
            row[alias] = _join_if_sequence(value)
            continue
        if key in row:
            continue
        if key.startswith("Parameter Value[") or key.startswith("Comment["):
            row[key] = _join_if_sequence(value)
        elif key in {"Assay Name", "MS Assay Name", "Protocol REF", "Raw Data File", "Term Source REF", "Term Accession Number", "Unit"}:
            row[key] = _join_if_sequence(value)
        else:
            row[key] = _stringify_scalar(value) if not isinstance(value, (list, tuple, set)) else _join_if_sequence(value)
    return row



def records_to_dataframe(records: Iterable[Any], project: str | None = None, mission: str | None = None) -> pd.DataFrame:
    rows = [record_to_canonical_row(record, project=project, mission=mission) for record in records]
    df = pd.DataFrame(rows)
    for col in BASE_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    ordered = BASE_COLUMNS + [c for c in df.columns if c not in BASE_COLUMNS]
    return df[ordered].copy()



def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.rename(columns={col: COLUMN_ALIASES.get(col, col) for col in df.columns})
    for col in BASE_COLUMNS:
        if col not in renamed.columns:
            renamed[col] = ""
    ordered = BASE_COLUMNS + [c for c in renamed.columns if c not in BASE_COLUMNS]
    return renamed[ordered].copy()
