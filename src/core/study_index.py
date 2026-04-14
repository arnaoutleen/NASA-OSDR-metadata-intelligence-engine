"""
Study index cache for fast organ/assay-first discovery.

This module builds and queries a lightweight study-level JSON cache so that
organ- and assay-based discovery does not need to hit OSDR on every query.
The cache is used only as a discovery layer; downstream ranking still relies
on full live/cached retrieval through DataRetriever.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from src.core.constants import CONTROLLED_ASSAY_TYPES, CONTROLLED_TISSUES
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso8601(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _normalize_organ(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return CONTROLLED_TISSUES.get(text.lower(), text)


_ASSAY_ALIASES = {
    **CONTROLLED_ASSAY_TYPES,
    "rna sequencing": "RNA-Seq",
    "transcription profiling": "RNA-Seq",
    "proteome profiling": "Proteomics",
}


def _normalize_assay(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return _ASSAY_ALIASES.get(text.lower(), text)


@dataclass
class StudyIndexCache:
    client: OSDRClient
    resolver: MissionResolver
    index_path: Path

    def load(self) -> Dict[str, Any]:
        if not self.index_path.exists():
            return self.empty_index()
        try:
            with open(self.index_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get("studies", []), list):
                return data
        except Exception:
            pass
        return self.empty_index()

    @staticmethod
    def empty_index() -> Dict[str, Any]:
        return {
            "generated_at": "",
            "source": "osdr_studies_index",
            "version": 1,
            "study_count": 0,
            "studies": [],
        }

    def save(self, index: Dict[str, Any]) -> None:
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.index_path, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2)

    def is_stale(self, index: Dict[str, Any], max_age_days: int = 7) -> bool:
        generated = _parse_iso8601(index.get("generated_at", ""))
        if not generated:
            return True
        return datetime.now(timezone.utc) - generated > timedelta(days=max_age_days)

    def refresh_if_needed(self, max_age_days: int = 7, force: bool = False, verbose: bool = False) -> Dict[str, Any]:
        index = self.load()
        if force or self.is_stale(index, max_age_days=max_age_days):
            if verbose:
                reason = "forced refresh" if force else f"cache older than {max_age_days} day(s)"
                print(f"Refreshing study index ({reason})...")
            index = self.build(verbose=verbose)
            self.save(index)
        return index

    def build(self, verbose: bool = False) -> Dict[str, Any]:
        osd_ids = self.client.list_all_study_ids()
        studies: List[Dict[str, Any]] = []

        for i, osd_id in enumerate(osd_ids, 1):
            if verbose and i % 25 == 1:
                print(f"  indexing {i}/{len(osd_ids)}: {osd_id}")
            entry = self._build_entry(osd_id)
            if entry:
                studies.append(entry)

        studies.sort(key=lambda x: int(str(x["osd_id"]).replace("OSD-", "")))
        return {
            "generated_at": _utc_now_iso(),
            "source": "osdr_studies_index",
            "version": 1,
            "study_count": len(studies),
            "studies": studies,
        }

    def _build_entry(self, osd_id: str) -> Optional[Dict[str, Any]]:
        osd_id = self.client.normalize_osd_id(osd_id)
        try:
            study_json = self.client.fetch_study_json(osd_id, use_cache=True) or {}
        except Exception:
            study_json = {}

        samples = study_json.get("samples", []) or []
        assays = study_json.get("assays", []) or []

        organs = set()
        organisms = set()
        assay_types = set()
        mouse_names = set()

        for sample in samples:
            if not isinstance(sample, dict):
                continue
            organ = _normalize_organ(sample.get("material_type", ""))
            if organ:
                organs.add(organ)
            organism = str(sample.get("organism", "")).strip()
            if organism:
                organisms.add(organism)
            source_name = str(sample.get("source_name", "")).strip()
            if source_name:
                mouse_names.add(source_name)
            # Biodata API sample entries may expose measurement/factor info directly.
            for m in sample.get("measurement_types", []) or []:
                assay = _normalize_assay(m)
                if assay:
                    assay_types.add(assay)

        for assay in assays:
            if not isinstance(assay, dict):
                continue
            assay_type = _normalize_assay(assay.get("type", ""))
            if assay_type:
                assay_types.add(assay_type)

        assay_types_string = str(study_json.get("assay_types", "")).strip()
        if assay_types_string:
            for raw in assay_types_string.replace("     ", "|").split("|"):
                assay = _normalize_assay(raw)
                if assay:
                    assay_types.add(assay)

        title = str(study_json.get("title", "")).strip()
        description = str(study_json.get("description", "")).strip()

        try:
            mission = self.resolver.get_mission_for_osd(osd_id) or ""
        except Exception:
            mission = ""

        return {
            "osd_id": osd_id,
            "mission": mission,
            "title": title,
            "description": description,
            "organisms": sorted(organisms),
            "organs": sorted(organs),
            "assays": sorted(assay_types),
            "sample_count": len(samples),
            "mouse_count": len(mouse_names),
            "retrievable": bool(samples),
            "last_seen": _utc_now_iso(),
        }

    def query(
        self,
        organ: Optional[str] = None,
        assay: Optional[str] = None,
        mission: Optional[str] = None,
        retrievable_only: bool = True,
    ) -> List[Dict[str, Any]]:
        index = self.load()
        studies = index.get("studies", []) or []

        organ_norm = _normalize_organ(organ) if organ else None
        assay_norm = _normalize_assay(assay) if assay else None
        mission_norm = mission.strip() if mission else None

        results = []
        for study in studies:
            if retrievable_only and not study.get("retrievable", False):
                continue
            if mission_norm and study.get("mission") != mission_norm:
                continue
            if organ_norm and organ_norm not in set(study.get("organs", [])):
                continue
            if assay_norm and assay_norm not in set(study.get("assays", [])):
                continue
            results.append(study)
        return results

    def list_organs(self, mission: Optional[str] = None) -> List[str]:
        studies = self.query(mission=mission, retrievable_only=True)
        organs = sorted({organ for study in studies for organ in study.get("organs", []) if organ})
        return organs

    def list_assays(self, mission: Optional[str] = None) -> List[str]:
        studies = self.query(mission=mission, retrievable_only=True)
        assays = sorted({assay for study in studies for assay in study.get("assays", []) if assay})
        return assays
