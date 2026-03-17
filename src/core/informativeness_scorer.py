"""
NASA OSDR Metadata Intelligence Engine - Informativeness Scoring

Provides two ranking tables:

Table 1 -- **Sample Informativeness** (per OSD / project):
    For each sample, how many assays and data files are available?
    Ranked by data availability within each OSD.

Table 2 -- **Mouse Informativeness** (per project + OSD):
    For each source_name within an OSD, how many organs x assays are
    available? Ranked by an overall informativeness score.

Informativeness Score Formula (Table 2):
    ``score = num_organs * num_distinct_assay_types + log2(1 + total_data_files)``
"""

import json
import math
from collections import defaultdict
from typing import Dict, List, Optional

import pandas as pd

from src.core.constants import CONTROLLED_ASSAY_TYPES, CONTROLLED_TISSUES
from src.core.data_retriever import SampleRecord


def _normalize_assay(name: str) -> str:
    lookup = name.lower().strip()
    return CONTROLLED_ASSAY_TYPES.get(lookup, name.strip())


def _normalize_tissue(name: str) -> str:
    lookup = name.lower().strip()
    return CONTROLLED_TISSUES.get(lookup, name.strip())


def _looks_pooled(text: Optional[str]) -> bool:
    if not text:
        return False
    t = text.strip().lower()
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


def _is_pooled_record(record: SampleRecord) -> bool:
    return _looks_pooled(getattr(record, "source_name", "")) or _looks_pooled(
        getattr(record, "sample_name", "")
    )


# ======================================================================
# Table 1 -- Sample-Level Informativeness
# ======================================================================


class SampleInformativenessScorer:
    """Rank non-pooled samples within OSD(s) by data availability."""

    def score(
        self,
        records: List[SampleRecord],
        project: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Generate the sample-level informativeness table.

        Args:
            records: List of :class:`SampleRecord` (may span multiple OSDs).
            project: Optional project name to include as a column.

        Returns:
            DataFrame sorted by ``num_assays`` DESC, ``data_files_count`` DESC,
            grouped by ``source_name`` within each OSD.
        """
        rows = []
        for r in records:
            if _is_pooled_record(r):
                continue

            assay_types = sorted({_normalize_assay(a) for a in r.measurement_types})
            rows.append({
                "project": project,
                "OSD": r.osd,
                "source_name": r.source_name,
                "sample_name": r.sample_name,
                "organ": _normalize_tissue(r.material_type),
                "assay_types": " | ".join(assay_types) if assay_types else "",
                "num_assays": len(assay_types),
                "data_files_count": len(r.data_files),
                "measurement_types": " | ".join(sorted(r.measurement_types)),
                "technology_types": " | ".join(sorted(r.technology_types)),
                "device_platforms": " | ".join(sorted(r.device_platforms)),
            })

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        # Sort: most data-rich first, then group by source_name within OSD
        df = df.sort_values(
            by=["project", "OSD", "num_assays", "data_files_count", "source_name"],
            ascending=[True, True, False, False, True],
        ).reset_index(drop=True)

        # Add rank within each OSD
        df["informativeness_rank"] = (
            df.groupby(["project", "OSD"])["num_assays"]
            .rank(method="dense", ascending=False)
            .astype(int)
        )

        return df


# ======================================================================
# Table 2 -- Mouse-Level Informativeness
# ======================================================================


class MouseInformativenessScorer:
    """
    Rank source_name + OSD combinations within a project by data coverage.

    Unlike the older implementation, this scorer does not collapse a
    source_name across multiple OSDs. Each (source_name, osd) pair gets
    its own row. Pooled samples are excluded.
    """

    def score(
        self,
        records: List[SampleRecord],
        project: str,
    ) -> pd.DataFrame:
        """
        Generate the mouse-level informativeness table.

        Args:
            records: List of :class:`SampleRecord` spanning all OSDs
                     in the project.
            project: Project name (included in output for labelling).

        Returns:
            DataFrame sorted by ``informativeness_score`` DESC.
        """
        grouped: Dict[tuple[str, str], List[SampleRecord]] = defaultdict(list)
        for r in records:
            if _is_pooled_record(r):
                continue

            source = r.source_name.strip()
            if not source:
                continue

            grouped[(source, r.osd)].append(r)

        rows = []
        for (source_name, osd), recs in grouped.items():
            organs: Dict[str, List[str]] = defaultdict(list)
            all_assays = set()
            total_files = 0

            for r in recs:
                organ = _normalize_tissue(r.material_type)
                for a in r.measurement_types:
                    normalized = _normalize_assay(a)
                    all_assays.add(normalized)
                    if normalized not in organs[organ]:
                        organs[organ].append(normalized)
                total_files += len(r.data_files)

            num_organs = len(organs)
            num_distinct_assays = len(all_assays)
            score = (
                num_organs * num_distinct_assays
                + math.log2(1 + total_files)
            )

            assays_per_organ = {
                organ: sorted(assay_list)
                for organ, assay_list in sorted(organs.items())
            }

            rows.append({
                "project": project,
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
