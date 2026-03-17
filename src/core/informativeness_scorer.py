
"""
NASA OSDR Metadata Intelligence Engine - Informativeness Scoring
"""

import json
import math
from collections import defaultdict
from typing import Any, Dict, List, Optional

import pandas as pd

from src.core.constants import CONTROLLED_ASSAY_TYPES, CONTROLLED_TISSUES


def _normalize_assay(name: str) -> str:
    if not name:
        return ""
    lookup = str(name).lower().strip()
    return CONTROLLED_ASSAY_TYPES.get(lookup, str(name).strip())


def _normalize_tissue(name: str) -> str:
    if not name:
        return ""
    lookup = str(name).lower().strip()
    return CONTROLLED_TISSUES.get(lookup, str(name).strip())


def _get(record: Any, key: str, default: Any = None) -> Any:
    if isinstance(record, dict):
        return record.get(key, default)
    return getattr(record, key, default)


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        # handle JSON-ish list string
        if text.startswith("[") and text.endswith("]"):
            text = text[1:-1]
        parts = [p.strip().strip("'").strip('"') for p in text.split("|")]
        return [p for p in parts if p and p.lower() != "nan"]
    return [str(value).strip()]


def _looks_pooled(text: Optional[str]) -> bool:
    if not text:
        return False
    t = str(text).strip().lower()
    pooled_markers = [
        "pool",
        "pooled",
        "empty pool",
        "not applicable",
        "n/a",
        "pool of all flt and gc samples",
        "pool of all flt samples",
        "pool of all gc samples",
    ]
    return any(marker in t for marker in pooled_markers)


def _is_pooled_record(record: Any) -> bool:
    return _looks_pooled(_get(record, "source_name", "")) or _looks_pooled(
        _get(record, "sample_name", "") or _get(record, "sample_id", "")
    )


class SampleInformativenessScorer:
    """Rank non-pooled samples within OSD(s) by data availability."""

    def score(
        self,
        records: List[Any],
        project: Optional[str] = None,
    ) -> pd.DataFrame:
        rows = []
        for r in records:
            if _is_pooled_record(r):
                continue

            assay_types = sorted({_normalize_assay(a) for a in _as_list(_get(r, "measurement_types", [])) if a})
            rec_project = project or _get(r, "project") or _get(r, "mission") or _get(r, "RR_mission") or _get(r, "osd_id") or _get(r, "osd")
            osd = _get(r, "osd") or _get(r, "osd_id") or ""
            rows.append({
                "project": rec_project,
                "OSD": osd,
                "source_name": _get(r, "source_name", ""),
                "sample_name": _get(r, "sample_name", "") or _get(r, "sample_id", ""),
                "organ": _normalize_tissue(_get(r, "material_type", "")),
                "assay_types": " | ".join(assay_types) if assay_types else "",
                "num_assays": len(assay_types),
                "data_files_count": len(_as_list(_get(r, "data_files", []))),
                "measurement_types": " | ".join(sorted(_as_list(_get(r, "measurement_types", [])))),
                "technology_types": " | ".join(sorted(_as_list(_get(r, "technology_types", [])))),
                "device_platforms": " | ".join(sorted(_as_list(_get(r, "device_platforms", [])))),
            })

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        df = df.sort_values(
            by=["project", "OSD", "num_assays", "data_files_count", "source_name"],
            ascending=[True, True, False, False, True],
        ).reset_index(drop=True)

        df["informativeness_rank"] = (
            df.groupby(["project", "OSD"])["num_assays"]
            .rank(method="dense", ascending=False)
            .astype(int)
        )

        return df


class MouseInformativenessScorer:
    """Rank source_name + OSD combinations within a project by data coverage."""

    def score(
        self,
        records: List[Any],
        project: Optional[str] = None,
    ) -> pd.DataFrame:
        grouped: Dict[tuple[str, str, str], List[Any]] = defaultdict(list)
        for r in records:
            if _is_pooled_record(r):
                continue

            source = str(_get(r, "source_name", "")).strip()
            osd = _get(r, "osd") or _get(r, "osd_id") or ""
            rec_project = project or _get(r, "project") or _get(r, "mission") or _get(r, "RR_mission") or osd
            if not source or not osd:
                continue

            grouped[(str(rec_project), source, str(osd))].append(r)

        rows = []
        for (rec_project, source_name, osd), recs in grouped.items():
            organs: Dict[str, List[str]] = defaultdict(list)
            all_assays = set()
            total_files = 0

            for r in recs:
                organ = _normalize_tissue(_get(r, "material_type", "")) or "Unknown"
                for a in _as_list(_get(r, "measurement_types", [])):
                    normalized = _normalize_assay(a)
                    if not normalized:
                        continue
                    all_assays.add(normalized)
                    if normalized not in organs[organ]:
                        organs[organ].append(normalized)
                total_files += len(_as_list(_get(r, "data_files", [])))

            num_organs = len(organs)
            num_distinct_assays = len(all_assays)
            score = num_organs * num_distinct_assays + math.log2(1 + total_files)

            assays_per_organ = {
                organ: sorted(assay_list)
                for organ, assay_list in sorted(organs.items())
            }

            rows.append({
                "project": rec_project,
                "source_name": source_name,
                "osd": osd,
                "num_organs": num_organs,
                "organs_list": " | ".join(sorted(organs.keys())),
                "num_total_assays": num_distinct_assays,
                "assays_per_organ": json.dumps(assays_per_organ),
                "total_data_files": total_files,
                "informativeness_score": round(score, 2),
            })

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        df = df.sort_values(
            by=["project", "osd", "informativeness_score", "num_organs", "num_total_assays", "source_name"],
            ascending=[True, True, False, False, False, True],
        ).reset_index(drop=True)

        df["informativeness_rank"] = (
            df.groupby(["project", "osd"])["informativeness_score"]
            .rank(method="dense", ascending=False)
            .astype(int)
        )

        return df
