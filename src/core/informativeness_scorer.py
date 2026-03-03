"""
NASA OSDR Metadata Intelligence Engine - Informativeness Scoring

Provides two ranking tables:

Table 1 -- **Sample Informativeness** (per OSD):
    For each sample, how many assays and data files are available?
    Ranked by data availability within each OSD.

Table 2 -- **Mouse Informativeness** (per Mission):
    Across all OSDs in a mission, how many organs x assays does each
    mouse have?  Ranked by an overall informativeness score.

Informativeness Score Formula (Table 2):
    ``score = num_organs * num_distinct_assay_types + log2(1 + total_data_files)``
"""

import json
import math
from collections import defaultdict
from typing import Dict, List

import pandas as pd

from src.core.constants import CONTROLLED_ASSAY_TYPES, CONTROLLED_TISSUES
from src.core.data_retriever import SampleRecord


def _normalize_assay(name: str) -> str:
    lookup = name.lower().strip()
    return CONTROLLED_ASSAY_TYPES.get(lookup, name.strip())


def _normalize_tissue(name: str) -> str:
    lookup = name.lower().strip()
    return CONTROLLED_TISSUES.get(lookup, name.strip())


# ======================================================================
# Table 1 -- Sample-Level Informativeness
# ======================================================================


class SampleInformativenessScorer:
    """Rank samples within OSD(s) by data availability."""

    def score(self, records: List[SampleRecord]) -> pd.DataFrame:
        """
        Generate the sample-level informativeness table.

        Args:
            records: List of :class:`SampleRecord` (may span multiple OSDs).

        Returns:
            DataFrame sorted by ``num_assays`` DESC, ``data_files_count`` DESC,
            grouped by ``source_name``.
        """
        rows = []
        for r in records:
            assay_types = sorted({_normalize_assay(a) for a in r.measurement_types})
            rows.append({
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
            by=["OSD", "num_assays", "data_files_count", "source_name"],
            ascending=[True, False, False, True],
        ).reset_index(drop=True)

        # Add rank within each OSD
        df["informativeness_rank"] = (
            df.groupby("OSD")["num_assays"]
            .rank(method="dense", ascending=False)
            .astype(int)
        )

        return df


# ======================================================================
# Table 2 -- Mouse-Level Informativeness
# ======================================================================


class MouseInformativenessScorer:
    """
    Rank mice across OSDs within a mission by overall data coverage.

    Cross-OSD linking is performed by matching ``source_name`` directly.
    For missions where source_names are consistent across OSDs (e.g.,
    ``RR1_FLT_M23`` appearing in eye, kidney, and muscle OSDs), one
    mouse will accumulate multiple organs.  For missions where
    different animals were used per organ study, each mouse will
    appear with a single organ.
    """

    def score(
        self,
        records: List[SampleRecord],
        mission: str,
    ) -> pd.DataFrame:
        """
        Generate the mouse-level informativeness table.

        Args:
            records: List of :class:`SampleRecord` spanning all OSDs
                     in the mission.
            mission: Mission name (included in output for labelling).

        Returns:
            DataFrame sorted by ``informativeness_score`` DESC.
        """
        # Group records by source_name (the animal-level key)
        mice: Dict[str, List[SampleRecord]] = defaultdict(list)
        for r in records:
            # Skip pool/empty entries
            source = r.source_name.strip()
            if not source or source.lower() in (
                "empty pool", "not applicable", "n/a",
                "pool of all flt and gc samples",
                "pool of all flt samples",
                "pool of all gc samples",
            ):
                continue
            mice[source].append(r)

        rows = []
        for source_name, recs in mice.items():
            organs: Dict[str, List[str]] = defaultdict(list)
            all_assays = set()
            total_files = 0
            osds_seen = set()

            for r in recs:
                organ = _normalize_tissue(r.material_type)
                osds_seen.add(r.osd)
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

            # Build assays_per_organ dict
            assays_per_organ = {
                organ: sorted(assay_list)
                for organ, assay_list in sorted(organs.items())
            }

            rows.append({
                "mission": mission,
                "source_name": source_name,
                "osds": " | ".join(sorted(osds_seen)),
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
            by=["informativeness_score", "num_organs", "num_total_assays"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

        return df
