#!/usr/bin/env python3
"""Rank OSDR records after discovering studies through a cached organ-first index."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.data_retriever import DataRetriever, SampleRecord
from src.core.informativeness_scorer import MouseInformativenessScorer, SampleInformativenessScorer
from src.core.isa_parser import ISAParser
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.core.study_index import StudyIndexCache
from src.utils.config import get_default_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover studies by organ and rank matching mice, samples, and assays",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
    python -m cli.rank_by_organ --organ liver
    python -m cli.rank_by_organ --organ retina --mission RR-1
    python -m cli.rank_by_organ --list-organs
    python -m cli.rank_by_organ --organ kidney --refresh-index --sample-output outputs/rankings/kidney_samples.csv
""",
    )
    parser.add_argument("--organ", type=str, help="Organ/tissue to search for")
    parser.add_argument("--assay", type=str, default=None, help="Optional assay type to further restrict matching studies")
    parser.add_argument("--mission", type=str, default=None, help="Optional mission filter")
    parser.add_argument("--list-organs", action="store_true", help="List discoverable organs from the local study index")
    parser.add_argument("--list-assays", action="store_true", help="List discoverable assays from the local study index")
    parser.add_argument("--refresh-index", action="store_true", help="Force a refresh of the study index from OSDR")
    parser.add_argument("--max-age-days", type=int, default=7, help="Refresh the index if it is older than this many days")
    parser.add_argument("--index-path", type=Path, default=None, help="Path to the local study index JSON")
    parser.add_argument("-o", "--output", dest="mouse_output", type=Path, default=None, help="Mouse ranking CSV output path")
    parser.add_argument("--sample-output", type=Path, default=None, help="Sample ranking CSV output path")
    parser.add_argument("--assay-output", type=Path, default=None, help="Assay summary CSV output path")
    parser.add_argument("--studies-output", type=Path, default=None, help="Matched studies summary CSV output path")
    parser.add_argument("-q", "--quiet", action="store_true")
    return parser.parse_args()


def _normalize_slug(text: str) -> str:
    return "_".join((text or "").strip().lower().replace("/", " ").split())


def _filter_records_by_organ(records: Iterable[SampleRecord], organ: str) -> List[SampleRecord]:
    organ_lower = organ.lower().strip()
    return [r for r in records if str(r.material_type).lower().strip() == organ_lower]


def _build_assay_summary(records: List[SampleRecord], mission_label: str) -> pd.DataFrame:
    rows = []
    for r in records:
        assays = sorted(set(r.measurement_types)) or [""]
        for assay in assays:
            rows.append({
                "mission": mission_label,
                "OSD": r.osd,
                "source_name": r.source_name,
                "sample_name": r.sample_name,
                "organ": r.material_type,
                "assay_type": assay,
                "data_files_count": len(r.data_files),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    grouped = (
        df.groupby(["mission", "OSD", "organ", "assay_type"], dropna=False)
        .agg(
            num_matching_samples=("sample_name", "nunique"),
            num_matching_mice=("source_name", "nunique"),
            total_data_files=("data_files_count", "sum"),
        )
        .reset_index()
    )
    grouped = grouped.sort_values(
        by=["num_matching_samples", "num_matching_mice", "total_data_files", "OSD", "assay_type"],
        ascending=[False, False, False, True, True],
    ).reset_index(drop=True)
    grouped["informativeness_rank"] = (
        grouped.groupby("OSD")["num_matching_samples"].rank(method="dense", ascending=False).astype(int)
    )
    return grouped


def main() -> int:
    args = parse_args()

    if not args.list_organs and not args.list_assays and not args.organ:
        print("Error: provide --organ, --list-organs, or --list-assays", file=sys.stderr)
        return 1

    defaults = get_default_paths()
    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=parser, resolver=resolver)

    index_path = args.index_path or (defaults["cache_dir"].parent / "study_index.json")
    study_cache = StudyIndexCache(client=client, resolver=resolver, index_path=index_path)
    study_cache.refresh_if_needed(max_age_days=args.max_age_days, force=args.refresh_index, verbose=not args.quiet)

    if args.list_organs:
        organs = study_cache.list_organs(mission=args.mission)
        for organ in organs:
            print(organ)
        return 0

    if args.list_assays:
        assays = study_cache.list_assays(mission=args.mission)
        for assay in assays:
            print(assay)
        return 0

    matches = study_cache.query(organ=args.organ, assay=args.assay, mission=args.mission, retrievable_only=True)
    if not matches:
        print("No studies matched the requested organ/assay filters.", file=sys.stderr)
        return 1

    matched_osds = [entry["osd_id"] for entry in matches]
    matched_organs = {entry["osd_id"]: entry.get("organs", []) for entry in matches}

    if not args.quiet:
        print(f"Matched {len(matched_osds)} study/studies for organ '{args.organ}'")
        for entry in matches:
            assays = ", ".join(entry.get("assays", []))
            print(f"  - {entry['osd_id']} ({entry.get('mission') or 'unknown mission'}): {assays}")

    all_records: List[SampleRecord] = []
    for i, osd_id in enumerate(matched_osds, 1):
        if not args.quiet:
            print(f"  [{i}/{len(matched_osds)}] retrieving {osd_id}...", end=" ", flush=True)
        try:
            recs = retriever.retrieve_osd(osd_id)
            all_records.extend(recs)
            if not args.quiet:
                print(f"({len(recs)} samples)")
        except Exception as e:
            if not args.quiet:
                print(f"(error: {e})")

    if not all_records:
        print("Error: no sample records retrieved for matched studies.", file=sys.stderr)
        return 1

    organ_filtered = _filter_records_by_organ(all_records, args.organ)
    if not organ_filtered:
        print(f"Error: no retrieved samples matched organ '{args.organ}'.", file=sys.stderr)
        return 1

    mission_label = args.mission or f"organ_query:{args.organ}"
    slug = _normalize_slug(args.organ)
    rankings_dir = defaults["output_dir"] / "rankings"
    rankings_dir.mkdir(parents=True, exist_ok=True)

    mouse_output = args.mouse_output or (rankings_dir / f"mouse_ranking_{slug}.csv")
    sample_output = args.sample_output or (rankings_dir / f"sample_ranking_{slug}.csv")
    assay_output = args.assay_output or (rankings_dir / f"assay_ranking_{slug}.csv")
    studies_output = args.studies_output or (rankings_dir / f"matched_studies_{slug}.csv")

    mouse_df = MouseInformativenessScorer().score(organ_filtered, mission_label)
    sample_df = SampleInformativenessScorer().score(organ_filtered)
    assay_df = _build_assay_summary(organ_filtered, mission_label)

    mouse_output.parent.mkdir(parents=True, exist_ok=True)
    sample_output.parent.mkdir(parents=True, exist_ok=True)
    assay_output.parent.mkdir(parents=True, exist_ok=True)
    studies_output.parent.mkdir(parents=True, exist_ok=True)

    mouse_df.to_csv(mouse_output, index=False)
    sample_df.to_csv(sample_output, index=False)
    assay_df.to_csv(assay_output, index=False)

    studies_df = pd.DataFrame([
        {
            "OSD": entry["osd_id"],
            "mission": entry.get("mission", ""),
            "matched_organs": " | ".join(entry.get("organs", [])),
            "num_matching_samples": sum(1 for r in organ_filtered if r.osd == entry["osd_id"]),
            "num_matching_mice": len({r.source_name for r in organ_filtered if r.osd == entry["osd_id"] and r.source_name}),
            "assay_types": " | ".join(entry.get("assays", [])),
            "title": entry.get("title", ""),
        }
        for entry in matches
    ])
    studies_df.to_csv(studies_output, index=False)

    if not args.quiet:
        print(f"\nMouse ranking table: {len(mouse_df)} rows -> {mouse_output}")
        print(f"Sample ranking table: {len(sample_df)} rows -> {sample_output}")
        print(f"Assay summary table: {len(assay_df)} rows -> {assay_output}")
        print(f"Matched studies summary: {len(studies_df)} rows -> {studies_output}")
        print(f"Study index used: {index_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
