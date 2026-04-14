
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

    Aggregates across ALL OSDs — a mouse that donated liver to OSD-48 AND kidney to
    OSD-168 is treated as one animal with two organs across two studies.

    Scoring formula:
        score = n_organs * n_distinct_assay_types + log2(1 + n_sample_rows)

    Pooled samples are excluded.

    Output columns (one row per mouse):
        osd_ids               pipe-separated list of all OSDs the mouse appears in
        pulled_at             earliest pulled_at timestamp across all OSDs
        mission               rocket/vehicle (SpaceX-4 etc.)
        project               research project (RR-1 etc.)
        mouse_id              payload-prefixed unique identifier
        source_name           original mouse ID from ISA-Tab
        n_organs              number of distinct organs sampled
        n_assay_types         number of distinct assay categories
        n_sample_rows         total rows in sample_metadata for this mouse
        informativeness_score
        informativeness_rank  1 = most informative, ranked within (mission, project)
        sample_inventory      JSON list: one entry per sample row
                              [{sample_id, osd_id, material_type, assay_category, assay_name}]
        organ_assay_inventory JSON list: one entry per organ × OSD combination
                              [{organ, assay_types (pipe-sep), osd_id}]
    """

    _SKIP_ORGANS = {"not applicable", "n/a", "na", "", "nan"}

    @staticmethod
    def _is_pooled(mouse_id: str) -> bool:
        t = str(mouse_id).lower()
        return any(m in t for m in ["pool", "pooled", "not appl", "empty"])

    def score(self, sample_df: "pd.DataFrame") -> "pd.DataFrame":
        import math, json
        import pandas as pd

        required = {"mouse_id", "material_type", "assay_category"}
        missing = required - set(sample_df.columns)
        if missing:
            raise ValueError(f"sample_df missing columns: {missing}")

        # Drop pooled rows
        df = sample_df[~sample_df["mouse_id"].apply(self._is_pooled)].copy()

        if df.empty:
            return pd.DataFrame(columns=[
                "osd_ids", "pulled_at", "mission", "project", "mouse_id", "source_name",
                "n_organs", "n_assay_types", "n_sample_rows",
                "informativeness_score", "informativeness_rank",
                "sample_inventory", "organ_assay_inventory",
            ])

        df["_organ"] = df["material_type"].fillna("").str.strip()
        df["_organ_valid"] = ~df["_organ"].str.lower().isin(self._SKIP_ORGANS)

        rows = []
        for mouse_id, grp in df.groupby("mouse_id"):

            # ── Basic counts ────────────────────────────────────────────────
            valid_mask  = grp["_organ_valid"]
            valid_organs = grp.loc[valid_mask, "_organ"].unique()
            assay_cats   = grp["assay_category"].dropna().unique()
            n_organs     = len(valid_organs)
            n_assay_types = len(assay_cats)
            n_rows       = len(grp)
            score        = n_organs * n_assay_types + math.log2(1 + n_rows)

            # ── OSD list ────────────────────────────────────────────────────
            osd_ids = sorted(grp["osd_id"].dropna().unique().tolist()) if "osd_id" in grp else []

            # ── Sample inventory ────────────────────────────────────────────
            # One entry per row in sample_df: {sample_id, osd_id, material_type, assay_category, assay_name}
            sample_inv = []
            for _, srow in grp.iterrows():
                entry = {
                    "sample_id":     str(srow.get("sample_id", srow.get("sample_name", ""))),
                    "osd_id":        str(srow.get("osd_id", "")),
                    "material_type": str(srow.get("material_type", "")),
                    "assay_category": str(srow.get("assay_category", "")),
                    "assay_name":    str(srow.get("assay_name", "")),
                }
                sample_inv.append(entry)

            # ── Organ × assay × OSD inventory ───────────────────────────────
            # Structure: one entry per organ, with a nested "assays" list.
            # Each assays entry: {osd_id, assay_types, assay_names}
            # Grouping by organ first means an organ never appears twice at
            # the top level, even when it was sampled in multiple OSDs.
            #
            # organ_data = { organ: { osd_id: {assay_cats, assay_names} } }
            organ_data: dict = {}
            for _, srow in grp[grp["_organ_valid"]].iterrows():
                organ = str(srow["_organ"])
                osd   = str(srow.get("osd_id", ""))
                acat  = str(srow.get("assay_category", ""))
                aname = str(srow.get("assay_name", ""))
                if organ not in organ_data:
                    organ_data[organ] = {}
                if osd not in organ_data[organ]:
                    organ_data[organ][osd] = {"assay_cats": set(), "assay_names": set()}
                if acat:
                    organ_data[organ][osd]["assay_cats"].add(acat)
                if aname:
                    organ_data[organ][osd]["assay_names"].add(aname)

            organ_inv = []
            for organ in sorted(organ_data.keys()):
                osd_entries = []
                for osd in sorted(organ_data[organ].keys()):
                    d = organ_data[organ][osd]
                    osd_entries.append({
                        "osd_id":      osd,
                        "assay_types": " | ".join(sorted(d["assay_cats"])),
                        "assay_names": " | ".join(sorted(d["assay_names"])),
                    })
                organ_inv.append({
                    "organ":  organ,
                    "assays": osd_entries,
                })

            # ── Scalar metadata (take from first row — same mouse) ───────────
            first = grp.iloc[0]

            rows.append({
                "osd_ids":    " | ".join(osd_ids),
                "pulled_at":  grp["pulled_at"].min() if "pulled_at" in grp else "",
                "mission":    str(first.get("mission", "")) if "mission" in grp else "",
                "project":    str(first.get("project", "")) if "project" in grp else "",
                "mouse_id":   mouse_id,
                "source_name": str(first.get("source_name", "")),
                "n_organs":   n_organs,
                "n_assay_types": n_assay_types,
                "n_sample_rows": n_rows,
                "informativeness_score": round(score, 3),
                "sample_inventory":    json.dumps(sample_inv, ensure_ascii=False),
                "organ_assay_inventory": json.dumps(organ_inv, ensure_ascii=False),
            })

        out = pd.DataFrame(rows)
        if out.empty:
            return out

        # Rank within (mission, project) — ties get same rank
        out = out.sort_values(
            by=["mission", "project", "informativeness_score", "n_organs", "n_assay_types", "mouse_id"],
            ascending=[True, True, False, False, False, True],
        ).reset_index(drop=True)

        out["informativeness_rank"] = (
            out.groupby(["mission", "project"])["informativeness_score"]
            .rank(method="dense", ascending=False)
            .astype(int)
        )

        # Reorder columns to match MOUSE_LEVEL_COLUMNS schema
        front = ["osd_ids", "pulled_at", "mission", "project", "mouse_id", "source_name",
                 "n_organs", "n_assay_types", "n_sample_rows",
                 "informativeness_score", "informativeness_rank",
                 "sample_inventory", "organ_assay_inventory"]
        out = out[[c for c in front if c in out.columns]]

        return out
