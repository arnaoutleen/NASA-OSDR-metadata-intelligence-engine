
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


class MouseRankerFromExport:
    """
    Rank mice directly from the sample_metadata DataFrame produced by run_full_export.

    This is the preferred scorer when the new export pipeline has been run,
    because sample_df already has per-row assay_category and material_type
    with pooled samples labelled. The old MouseInformativenessScorer works
    on raw SampleRecord objects from DataRetriever and is kept for compatibility.

    Scoring formula (same logic as MouseInformativenessScorer):
        score = n_organs * n_distinct_assay_types + log2(1 + n_sample_rows)

    A mouse with kidney + liver × RNA-seq + mass-spec scores higher than one
    with only liver × mass-spec.  Pooled samples are excluded.
    """

    # Values in material_type that mean "no real organ"
    _SKIP_ORGANS = {"not applicable", "n/a", "na", "", "nan"}

    @staticmethod
    def _is_pooled(mouse_id: str) -> bool:
        t = str(mouse_id).lower()
        return any(m in t for m in ["pool", "pooled", "not appl", "empty"])

    def score(self, sample_df: "pd.DataFrame") -> "pd.DataFrame":
        """
        Parameters
        ----------
        sample_df : pd.DataFrame
            The sample_metadata.csv DataFrame from run_full_export.

        Returns
        -------
        pd.DataFrame
            One row per mouse, sorted by informativeness_rank (1 = most informative).
            Columns:
                osd_id, payload, mission, mouse_id,
                n_organs, organs,
                n_assay_types, assay_types, assay_names,
                n_sample_rows,
                informativeness_score, informativeness_rank
        """
        import math
        import pandas as pd

        required = {"mouse_id", "material_type", "assay_category"}
        missing = required - set(sample_df.columns)
        if missing:
            raise ValueError(f"sample_df missing columns: {missing}")

        # Drop pooled rows
        mask = ~sample_df["mouse_id"].apply(self._is_pooled)
        df = sample_df[mask].copy()

        if df.empty:
            return pd.DataFrame(columns=[
                "osd_id", "pulled_at", "payload", "mission", "mouse_id",
                "n_organs", "organs", "n_assay_types", "assay_types", "assay_names",
                "n_sample_rows", "informativeness_score", "informativeness_rank",
            ])

        # Clean material_type
        df["_organ"] = df["material_type"].fillna("").str.strip()
        df["_organ_valid"] = ~df["_organ"].str.lower().isin(self._SKIP_ORGANS)

        rows = []
        for mouse_id, grp in df.groupby("mouse_id"):
            valid_organs = grp.loc[grp["_organ_valid"], "_organ"].unique()
            assay_cats   = grp["assay_category"].dropna().unique()
            assay_names  = grp["assay_name"].dropna().unique() if "assay_name" in grp else []

            n_organs     = len(valid_organs)
            n_assay_types = len(assay_cats)
            n_rows       = len(grp)
            score        = n_organs * n_assay_types + math.log2(1 + n_rows)

            rows.append({
                "osd_id":       grp["osd_id"].iloc[0] if "osd_id" in grp else "",
                "pulled_at":    grp["pulled_at"].iloc[0] if "pulled_at" in grp else "",
                "payload":      grp["payload"].iloc[0] if "payload" in grp else "",
                "mission":      grp["mission"].iloc[0] if "mission" in grp else "",
                "mouse_id":     mouse_id,
                "n_organs":     n_organs,
                "organs":       " | ".join(sorted(valid_organs)),
                "n_assay_types": n_assay_types,
                "assay_types":  " | ".join(sorted(assay_cats)),
                "assay_names":  " | ".join(sorted(str(a) for a in assay_names if a)),
                "n_sample_rows": n_rows,
                "informativeness_score": round(score, 3),
            })

        out = pd.DataFrame(rows)
        if out.empty:
            return out

        # Rank within (payload, mission, osd_id) group — ties get same rank
        out = out.sort_values(
            by=["payload", "mission", "osd_id", "informativeness_score",
                "n_organs", "n_assay_types", "mouse_id"],
            ascending=[True, True, True, False, False, False, True],
        ).reset_index(drop=True)

        out["informativeness_rank"] = (
            out.groupby(["payload", "mission", "osd_id"])["informativeness_score"]
            .rank(method="dense", ascending=False)
            .astype(int)
        )

        return out
