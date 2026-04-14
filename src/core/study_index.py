"""
NASA OSDR Metadata Intelligence Engine - Lightweight Study Index

Builds and queries a cached study-level JSON index that supports fast
organ-first and assay-first discovery without repeatedly scanning the live
OSDR catalog for every downstream question.

This index is intentionally lightweight and is only meant for discovery.
Final rankings should still be computed from fully retrieved sample records.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from src.core.constants import CONTROLLED_ASSAY_TYPES, CONTROLLED_TISSUES
from src.core.data_retriever import DataRetriever, SampleRecord
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.utils.config import get_default_paths

INDEX_VERSION = 1
INDEX_FILENAME = "study_index.json"


@dataclass
class StudyIndexConfig:
    path: Path
    max_age_days: int = 7


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def default_index_path() -> Path:
    defaults = get_default_paths()
    return defaults["cache_dir"].parent / INDEX_FILENAME


def normalize_assay(name: str) -> str:
    if not name:
        return ""
    lookup = str(name).strip().lower()
    return CONTROLLED_ASSAY_TYPES.get(lookup, str(name).strip())


def normalize_organ(name: str) -> str:
    if not name:
        return ""
    lookup = str(name).strip().lower()
    return CONTROLLED_TISSUES.get(lookup, str(name).strip())


def normalize_token(text: str) -> str:
    return " ".join(str(text).strip().lower().replace("_", " ").replace("-", " ").split())


def matches_token(value: str, query: str) -> bool:
    left = normalize_token(value)
    right = normalize_token(query)
    if not left or not right:
        return False
    return left == right or right in left or left in right


def build_study_summary(
    osd_id: str,
    records: List[SampleRecord],
    resolver: MissionResolver,
) -> Dict[str, Any]:
    organs = sorted({normalize_organ(r.material_type) for r in records if r.material_type})
    assays = sorted({normalize_assay(a) for r in records for a in r.measurement_types if a})
    sources = {r.source_name for r in records if str(r.source_name).strip()}
    assay_to_organs: Dict[str, set[str]] = {}
    organ_to_assays: Dict[str, set[str]] = {}
    total_data_files = 0

    for record in records:
        organ = normalize_organ(record.material_type)
        total_data_files += len(record.data_files)
        for assay in record.measurement_types:
            assay_norm = normalize_assay(assay)
            if not assay_norm:
                continue
            assay_to_organs.setdefault(assay_norm, set())
            if organ:
                assay_to_organs[assay_norm].add(organ)
            if organ:
                organ_to_assays.setdefault(organ, set()).add(assay_norm)

    return {
        "osd_id": OSDRClient.normalize_osd_id(osd_id),
        "mission": resolver.get_mission_for_osd(osd_id) or "Unknown",
        "organs": organs,
        "assays": assays,
        "assay_to_organs": {k: sorted(v) for k, v in sorted(assay_to_organs.items())},
        "organ_to_assays": {k: sorted(v) for k, v in sorted(organ_to_assays.items())},
        "sample_count": len(records),
        "mouse_count": len(sources),
        "total_data_files": total_data_files,
        "retrievable": True,
        "last_seen": _utc_now_iso(),
    }


def build_study_index(
    client: OSDRClient,
    resolver: MissionResolver,
    retriever: DataRetriever,
    output_path: Optional[Path] = None,
    osd_ids: Optional[Iterable[str]] = None,
    quiet: bool = False,
) -> Dict[str, Any]:
    output_path = output_path or default_index_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if osd_ids is None:
        osd_ids = client.list_all_study_ids()
    osd_ids = [OSDRClient.normalize_osd_id(osd) for osd in osd_ids]

    studies: List[Dict[str, Any]] = []
    failed: List[Dict[str, str]] = []

    total = len(osd_ids)
    for i, osd_id in enumerate(osd_ids, 1):
        if not quiet:
            print(f"[{i}/{total}] Indexing {osd_id}...", flush=True)
        try:
            records = retriever.retrieve_osd(osd_id)
            if not records:
                failed.append({"osd_id": osd_id, "reason": "No retrievable records"})
                continue
            studies.append(build_study_summary(osd_id, records, resolver))
        except Exception as exc:
            failed.append({"osd_id": osd_id, "reason": str(exc)})

    payload = {
        "generated_at": _utc_now_iso(),
        "version": INDEX_VERSION,
        "source": "osdr_study_index",
        "study_count": len(studies),
        "failed_count": len(failed),
        "studies": sorted(studies, key=lambda row: row["osd_id"]),
        "failed": failed,
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)

    return payload


def load_study_index(index_path: Optional[Path] = None) -> Dict[str, Any]:
    index_path = index_path or default_index_path()
    with open(index_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def index_is_stale(index_path: Optional[Path] = None, max_age_days: int = 7) -> bool:
    index_path = index_path or default_index_path()
    if not index_path.exists():
        return True
    try:
        payload = load_study_index(index_path)
        generated_at = payload.get("generated_at")
        if not generated_at:
            return True
        generated_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) - generated_dt > timedelta(days=max_age_days)
    except Exception:
        return True


def refresh_study_index_if_needed(
    client: OSDRClient,
    resolver: MissionResolver,
    retriever: DataRetriever,
    index_path: Optional[Path] = None,
    max_age_days: int = 7,
    force: bool = False,
    quiet: bool = False,
) -> Dict[str, Any]:
    index_path = index_path or default_index_path()
    if force or index_is_stale(index_path, max_age_days=max_age_days):
        return build_study_index(
            client=client,
            resolver=resolver,
            retriever=retriever,
            output_path=index_path,
            quiet=quiet,
        )
    return load_study_index(index_path)


def query_study_index(
    payload: Dict[str, Any],
    assays: Optional[Iterable[str]] = None,
    mission: Optional[str] = None,
    organ: Optional[str] = None,
    match: str = "all",
) -> List[Dict[str, Any]]:
    studies = payload.get("studies", [])
    requested_assays = [normalize_assay(a) for a in (assays or []) if str(a).strip()]
    requested_assays = [a for a in requested_assays if a]

    results: List[Dict[str, Any]] = []
    for study in studies:
        if mission and str(study.get("mission", "")).strip().lower() != str(mission).strip().lower():
            continue

        study_assays = study.get("assays", []) or []
        study_organs = study.get("organs", []) or []
        assay_to_organs = study.get("assay_to_organs", {}) or {}

        if requested_assays:
            if match == "all":
                if not all(any(matches_token(sa, ra) for sa in study_assays) for ra in requested_assays):
                    continue
            else:
                if not any(any(matches_token(sa, ra) for sa in study_assays) for ra in requested_assays):
                    continue

        if organ:
            organ_match = any(matches_token(o, organ) for o in study_organs)
            if not organ_match:
                continue
            if requested_assays:
                organ_norm = normalize_organ(organ)
                if match == "all":
                    if not all(any(matches_token(o, organ_norm) for o in assay_to_organs.get(ra, [])) for ra in requested_assays):
                        continue
                else:
                    if not any(any(matches_token(o, organ_norm) for o in assay_to_organs.get(ra, [])) for ra in requested_assays):
                        continue

        results.append(study)

    return sorted(results, key=lambda row: (row.get("mission", ""), row.get("osd_id", "")))
