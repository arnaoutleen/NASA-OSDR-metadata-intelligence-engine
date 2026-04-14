#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - Assay-first Discovery CLI

Uses a cached study index to answer assay-forward questions like:
- which studies have RNA-Seq?
- which organs have both RNA-Seq and Proteomics?
- within RR-1, which OSDs have methylation profiling?

When requested, it can also retrieve the matched studies and produce the same
mouse/sample ranking tables already used elsewhere in the repo.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.data_retriever import DataRetriever, SampleRecord
from src.core.informativeness_scorer import MouseInformativenessScorer, SampleInformativenessScorer
from src.core.isa_parser import ISAParser
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.core.study_index import (
    default_index_path,
    normalize_assay,
    normalize_organ,
    query_study_index,
    refresh_study_index_if_needed,
)
from src.utils.config import get_default_paths


def build_runtime() -> Tuple[OSDRClient, MissionResolver, DataRetriever]:
    defaults = get_default_paths()
    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=parser, resolver=resolver)
    return client, resolver, retriever


def slugify(text: str) -> str:
    return "_".join(" ".join(str(text).strip().lower().replace("-", " ").replace("_", " ").split()).split())


def list_available_assays(payload: Dict) -> List[str]:
    return sorted({assay for study in payload.get("studies", []) for assay in study.get("assays", []) if assay})


def build_studies_df(matches: List[Dict]) -> pd.DataFrame:
    rows = []
    for study in matches:
        rows.append({
            "OSD": study.get("osd_id", ""),
            "mission": study.get("mission", ""),
            "organs": " | ".join(study.get("organs", []) or []),
            "assays": " | ".join(study.get("assays", []) or []),
            "sample_count": study.get("sample_count", 0),
            "mouse_count": study.get("mouse_count", 0),
            "total_data_files": study.get("total_data_files", 0),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(by=["mission", "OSD"]).reset_index(drop=True)


def build_organs_df(matches: List[Dict], requested_assays: List[str]) -> pd.DataFrame:
    organ_map: Dict[str, Dict[str, object]] = {}
    for study in matches:
        osd = study.get("osd_id", "")
        mission = study.get("mission", "")
        assay_to_organs = study.get("assay_to_organs", {}) or {}
        for assay in requested_assays:
            matched_organs = assay_to_organs.get(assay, [])
            for organ in matched_organs:
                entry = organ_map.setdefault(organ, {
                    "organ": organ,
                    "missions": set(),
                    "osds": set(),
                    "requested_assays_present": set(),
                })
                entry["missions"].add(mission)
                entry["osds"].add(osd)
                entry["requested_assays_present"].add(assay)

    rows = []
    for organ, entry in organ_map.items():
        rows.append({
            "organ": organ,
            "num_studies": len(entry["osds"]),
            "missions": " | ".join(sorted(entry["missions"])),
            "osds": " | ".join(sorted(entry["osds"])),
            "requested_assays_present": " | ".join(sorted(entry["requested_assays_present"])),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(by=["num_studies", "organ"], ascending=[False, True]).reset_index(drop=True)


def retrieve_records(retriever: DataRetriever, osd_ids: Iterable[str], quiet: bool = False) -> List[SampleRecord]:
    records: List[SampleRecord] = []
    osd_ids = list(osd_ids)
    total = len(osd_ids)
    for i, osd_id in enumerate(osd_ids, 1):
        if not quiet:
            print(f"[{i}/{total}] Retrieving {osd_id}...", flush=True)
        try:
            records.extend(retriever.retrieve_osd(osd_id))
        except Exception as exc:
            if not quiet:
                print(f"  Skipped {osd_id}: {exc}", flush=True)
    return records


def filter_records(records: List[SampleRecord], assays: List[str], organ: str | None, match: str) -> List[SampleRecord]:
    filtered: List[SampleRecord] = []
    organ_norm = normalize_organ(organ) if organ else None
    for record in records:
        record_assays = {normalize_assay(a) for a in record.measurement_types if a}
        if assays:
            if match == "all" and not all(a in record_assays for a in assays):
                continue
            if match == "any" and not any(a in record_assays for a in assays):
                continue
        if organ_norm:
            if normalize_organ(record.material_type) != organ_norm:
                continue
        filtered.append(record)
    return filtered


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query the cached study index by assay or assay combinations, optionally retrieve matched studies, and emit organ/study summaries plus rankings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
    python -m cli.rank_by_assay --assay RNA-Seq
    python -m cli.rank_by_assay --assay RNA-Seq --assay Proteomics --match all
    python -m cli.rank_by_assay --assay RNA-Seq --organ liver --mission RR-1
    python -m cli.rank_by_assay --list-assays
""",
    )
    parser.add_argument("--assay", action="append", dest="assays", help="Requested assay type; may be passed multiple times.")
    parser.add_argument("--organ", type=str, help="Optional organ filter to require assay presence in a specific organ.")
    parser.add_argument("--mission", type=str, help="Optional mission filter (e.g. RR-1).")
    parser.add_argument("--match", choices=["all", "any"], default="all", help="Whether all requested assays must be present or any one of them.")
    parser.add_argument("--list-assays", action="store_true", help="Print discoverable assay types from the local study index.")
    parser.add_argument("--refresh-index", action="store_true", help="Force rebuild the cached study index before querying.")
    parser.add_argument("--max-age-days", type=int, default=7, help="Rebuild the study index if older than this many days.")
    parser.add_argument("--index-path", type=Path, default=default_index_path(), help="Path to study_index.json")
    parser.add_argument("--retrieve", action="store_true", help="Retrieve matched studies and emit mouse/sample ranking tables.")
    parser.add_argument("--studies-output", type=Path, help="Optional CSV path for the matched-studies summary.")
    parser.add_argument("--organs-output", type=Path, help="Optional CSV path for the organ summary.")
    parser.add_argument("--mouse-output", type=Path, help="Optional CSV path for the mouse ranking table (requires --retrieve).")
    parser.add_argument("--sample-output", type=Path, help="Optional CSV path for the sample ranking table (requires --retrieve).")
    parser.add_argument("-q", "--quiet", action="store_true", help="Reduce progress output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client, resolver, retriever = build_runtime()

    payload = refresh_study_index_if_needed(
        client=client,
        resolver=resolver,
        retriever=retriever,
        index_path=args.index_path,
        max_age_days=args.max_age_days,
        force=args.refresh_index,
        quiet=args.quiet,
    )

    if args.list_assays and not args.assays:
        assays = list_available_assays(payload)
        if not assays:
            print("No assays found in the study index.")
            return 1
        print("\nDiscoverable assays:\n")
        for assay in assays:
            print(f"- {assay}")
        return 0

    requested_assays = [normalize_assay(a) for a in (args.assays or []) if str(a).strip()]
    if not requested_assays:
        print("Error: provide at least one --assay or use --list-assays.", file=sys.stderr)
        return 1

    matches = query_study_index(
        payload,
        assays=requested_assays,
        mission=args.mission,
        organ=args.organ,
        match=args.match,
    )
    if not matches:
        scope = []
        if args.mission:
            scope.append(f"mission '{args.mission}'")
        if args.organ:
            scope.append(f"organ '{args.organ}'")
        scope_text = f" within {' and '.join(scope)}" if scope else ""
        print(f"No studies found for assay query {requested_assays}{scope_text}.")
        return 1

    studies_df = build_studies_df(matches)
    organs_df = build_organs_df(matches, requested_assays)

    assay_slug = "__".join(slugify(a) for a in requested_assays)
    organ_slug = f"_{slugify(args.organ)}" if args.organ else ""
    mission_slug = f"_{slugify(args.mission)}" if args.mission else ""
    label = f"{assay_slug}{organ_slug}{mission_slug}"
    defaults = get_default_paths()
    rankings_dir = defaults["output_dir"] / "rankings"
    rankings_dir.mkdir(parents=True, exist_ok=True)

    studies_output = args.studies_output or rankings_dir / f"assay_query_studies_{label}.csv"
    organs_output = args.organs_output or rankings_dir / f"assay_query_organs_{label}.csv"
    studies_output.parent.mkdir(parents=True, exist_ok=True)
    organs_output.parent.mkdir(parents=True, exist_ok=True)
    studies_df.to_csv(studies_output, index=False)
    organs_df.to_csv(organs_output, index=False)

    print(f"Matched studies: {len(studies_df)}")
    print(f"Study summary written to: {studies_output}")
    print(f"Organ summary written to: {organs_output}")

    if not args.quiet:
        print("\nMatched studies:\n")
        print(studies_df.head(20).to_string(index=False))
        if not organs_df.empty:
            print("\nMatching organs:\n")
            print(organs_df.head(20).to_string(index=False))

    if not args.retrieve:
        return 0

    osd_ids = [row["OSD"] for _, row in studies_df.iterrows()]
    all_records = retrieve_records(retriever, osd_ids=osd_ids, quiet=args.quiet)
    matched_records = filter_records(all_records, requested_assays, args.organ, args.match)
    if not matched_records:
        print("Matched studies were found in the index, but no retrievable sample records matched the requested filters.")
        return 1

    sample_df = SampleInformativenessScorer().score(matched_records)
    mission_label = args.mission if args.mission else f"assay_query:{' + '.join(requested_assays)}"
    mouse_df = MouseInformativenessScorer().score(matched_records, project=mission_label)

    mouse_output = args.mouse_output or rankings_dir / f"mouse_ranking_assay_{label}.csv"
    sample_output = args.sample_output or rankings_dir / f"sample_ranking_assay_{label}.csv"
    mouse_output.parent.mkdir(parents=True, exist_ok=True)
    sample_output.parent.mkdir(parents=True, exist_ok=True)
    mouse_df.to_csv(mouse_output, index=False)
    sample_df.to_csv(sample_output, index=False)

    print(f"Matched records retrieved: {len(matched_records)}")
    print(f"Mouse ranking written to: {mouse_output}")
    print(f"Sample ranking written to: {sample_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
