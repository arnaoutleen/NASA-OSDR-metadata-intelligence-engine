#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - Organ-first Mouse Ranking CLI

Discover OSD studies containing a requested organ/tissue, retrieve matching
records, and generate the same mouse-ranking table produced by ``cli.rank_mice``.

Usage:
    python -m cli.rank_by_organ --organ liver
    python -m cli.rank_by_organ --organ retina -o outputs/rankings/retina_mice.csv
    python -m cli.rank_by_organ --list-organs
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.data_retriever import DataRetriever, SampleRecord
from src.core.informativeness_scorer import MouseInformativenessScorer
from src.core.isa_parser import ISAParser
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.utils.config import get_default_paths


def normalize_token(text: str) -> str:
    return " ".join(str(text).strip().lower().replace("_", " ").replace("-", " ").split())


def organ_matches(record_organ: str, requested_organ: str) -> bool:
    rec = normalize_token(record_organ)
    req = normalize_token(requested_organ)
    if not rec or not req:
        return False
    return rec == req or req in rec or rec in req


def build_runtime() -> Tuple[OSDRClient, MissionResolver, DataRetriever]:
    defaults = get_default_paths()
    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=parser, resolver=resolver)
    return client, resolver, retriever


def discover_organs_across_osdr(retriever: DataRetriever, osd_ids: Iterable[str], quiet: bool = False) -> List[str]:
    organs = set()
    osd_ids = list(osd_ids)
    for i, osd in enumerate(osd_ids, 1):
        if not quiet:
            print(f"[{i}/{len(osd_ids)}] Scanning {osd} for organs...", flush=True)
        try:
            records = retriever.retrieve_osd(osd)
        except Exception as exc:
            if not quiet:
                print(f"  Skipped {osd}: {exc}")
            continue

        for record in records:
            if record.material_type:
                organs.add(record.material_type)

    return sorted(organs)


def find_matching_records(
    retriever: DataRetriever,
    resolver: MissionResolver,
    organ: str,
    osd_ids: Iterable[str],
    quiet: bool = False,
) -> Tuple[List[SampleRecord], pd.DataFrame]:
    requested = normalize_token(organ)
    matched_records: List[SampleRecord] = []
    study_rows = []

    osd_ids = list(osd_ids)
    total = len(osd_ids)
    for i, osd in enumerate(osd_ids, 1):
        if not quiet:
            print(f"[{i}/{total}] Retrieving {osd}...", flush=True)
        try:
            records = retriever.retrieve_osd(osd)
        except Exception as exc:
            if not quiet:
                print(f"  Error retrieving {osd}: {exc}")
            continue

        organ_records = [record for record in records if organ_matches(record.material_type, requested)]
        if not organ_records:
            continue

        assays = sorted({assay for record in organ_records for assay in record.measurement_types if assay})
        organ_names = sorted({record.material_type for record in organ_records if record.material_type})
        source_names = sorted({record.source_name for record in organ_records if record.source_name})
        study_rows.append({
            "OSD": osd,
            "mission": resolver.get_mission_for_osd(osd) or "Unknown",
            "matched_organs": " | ".join(organ_names),
            "num_matching_samples": len(organ_records),
            "num_matching_mice": len(source_names),
            "assay_types": " | ".join(assays),
        })
        matched_records.extend(organ_records)

    studies_df = pd.DataFrame(study_rows)
    if not studies_df.empty:
        studies_df = studies_df.sort_values(
            by=["mission", "OSD", "num_matching_mice"],
            ascending=[True, True, False],
        ).reset_index(drop=True)

    return matched_records, studies_df


def rank_matching_records(records: List[SampleRecord], organ_label: str) -> pd.DataFrame:
    scorer = MouseInformativenessScorer()
    return scorer.score(records, project=f"organ_query:{organ_label}")


def choose_organ_interactively(available_organs: List[str]) -> str:
    if not available_organs:
        raise RuntimeError("No organs could be discovered from the retrievable OSD studies.")

    print("\nAvailable organs/tissues:\n")
    for idx, organ in enumerate(available_organs, 1):
        print(f"{idx}. {organ}")

    chosen = input("\nEnter an organ: ").strip()
    if not chosen:
        raise ValueError("No organ entered.")
    return chosen


def print_study_summary(studies_df: pd.DataFrame) -> None:
    if studies_df.empty:
        print("\nNo matching studies found.")
        return
    print("\nMatching studies:\n")
    print(studies_df.to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query OSDR by organ and output a rank_mice-style mouse ranking table.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
    python -m cli.rank_by_organ --organ liver
    python -m cli.rank_by_organ --organ retina -o outputs/rankings/retina_mouse_ranking.csv
    python -m cli.rank_by_organ --list-organs
""",
    )
    parser.add_argument("--organ", type=str, help="Organ/tissue to search for (e.g., liver, retina, kidney)")
    parser.add_argument("--list-organs", action="store_true", help="Scan OSDR and print all discoverable organs")
    parser.add_argument(
        "--mission",
        type=str,
        help="Optional mission filter (e.g., RR-1). If provided, only OSDs from that mission are searched.",
    )
    parser.add_argument("-o", "--output", type=Path, dest="output_path", help="Output CSV path for the ranking table")
    parser.add_argument("--studies-output", type=Path, help="Optional CSV path for the matched-studies summary table")
    parser.add_argument("-q", "--quiet", action="store_true", help="Reduce progress output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client, resolver, retriever = build_runtime()

    if args.mission:
        osd_ids = resolver.resolve_mission(args.mission)
        if not osd_ids:
            print(f"Error: No OSDs found for mission '{args.mission}'", file=sys.stderr)
            return 1
    else:
        osd_ids = client.list_all_study_ids()
        if not osd_ids:
            print("Error: could not retrieve the OSD study catalog from OSDR.", file=sys.stderr)
            return 1

    if args.list_organs and not args.organ:
        organs = discover_organs_across_osdr(retriever, osd_ids, quiet=args.quiet)
        if not organs:
            print("No organs discovered.")
            return 1
        print("\nDiscoverable organs/tissues:\n")
        for organ in organs:
            print(f"- {organ}")
        return 0

    organ = args.organ
    if not organ:
        organs = discover_organs_across_osdr(retriever, osd_ids, quiet=args.quiet)
        organ = choose_organ_interactively(organs)

    matched_records, studies_df = find_matching_records(
        retriever=retriever,
        resolver=resolver,
        organ=organ,
        osd_ids=osd_ids,
        quiet=args.quiet,
    )

    if not matched_records:
        scope = f" within mission '{args.mission}'" if args.mission else ""
        print(f"\nNo studies found for organ '{organ}'{scope}.")
        return 1

    if not args.quiet:
        print_study_summary(studies_df)

    ranking_df = rank_matching_records(matched_records, organ_label=organ)
    if ranking_df.empty:
        print(f"\nStudies were found for '{organ}', but no rankable mouse rows were produced.")
        return 1

    defaults = get_default_paths()
    if args.output_path:
        output_path = args.output_path
    else:
        organ_slug = normalize_token(organ).replace(" ", "_")
        label = f"{args.mission}_{organ_slug}" if args.mission else organ_slug
        output_path = defaults["output_dir"] / "rankings" / f"mouse_ranking_{label}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ranking_df.to_csv(output_path, index=False)

    if args.studies_output:
        args.studies_output.parent.mkdir(parents=True, exist_ok=True)
        studies_df.to_csv(args.studies_output, index=False)

    print(f"\nMatched studies: {len(studies_df)}")
    print(f"Ranked mice: {len(ranking_df)}")
    print(f"Ranking output written to: {output_path}")
    if args.studies_output:
        print(f"Study summary written to: {args.studies_output}")

    if not args.quiet:
        preview_cols = [
            "mission",
            "source_name",
            "osds",
            "num_organs",
            "organs_list",
            "num_total_assays",
            "total_data_files",
            "informativeness_score",
        ]
        print("\nTop rows:\n")
        print(ranking_df[preview_cols].head(10).to_string(index=False))

    return 0


if __name__ == "__main__":
    sys.exit(main())
