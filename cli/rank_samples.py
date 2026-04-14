#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.data_retriever import DataRetriever
from src.core.informativeness_scorer import SampleInformativenessScorer
from src.core.isa_parser import ISAParser
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.utils.config import get_default_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank sample rows by data availability")
    parser.add_argument("osd_ids", nargs="*", default=[])
    parser.add_argument("--project", type=str, default=None)
    parser.add_argument("--mission", type=str, default=None, help="Alias for --project")
    parser.add_argument("-o", "--output", type=Path, dest="output_path")
    parser.add_argument("--format", choices=["csv", "json"], default="csv")
    parser.add_argument("-q", "--quiet", action="store_true")
    args = parser.parse_args()
    if not args.osd_ids and not args.project and not args.mission:
        parser.error("Provide OSD IDs or --project (or --mission)")
    return args


def main() -> int:
    args = parse_args()
    project = args.project or args.mission

    defaults = get_default_paths()
    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=parser, resolver=resolver)

    records = []
    if project:
        osds = resolver.resolve_mission(project)
        if not osds:
            print(f"Error: No OSDs found for project '{project}'", file=sys.stderr)
            return 1
        label = project.replace(" ", "_")
    else:
        osds = args.osd_ids
        label = "_".join(args.osd_ids) if len(args.osd_ids) <= 3 else "multi"

    for i, osd in enumerate(osds, 1):
        if not args.quiet:
            print(f"  [{i}/{len(osds)}] {osd}...", end=" ", flush=True)
        try:
            recs = retriever.retrieve_osd(osd)
            records.extend(recs)
            if not args.quiet:
                print(f"({len(recs)} samples)")
        except Exception as e:
            if not args.quiet:
                print(f"(error: {e})")

    if not records:
        print("Error: No sample records retrieved", file=sys.stderr)
        return 1

    scorer = SampleInformativenessScorer()
    df = scorer.score(records, project=project)

    ext = "json" if args.format == "json" else "csv"
    output_path = args.output_path or (defaults["output_dir"] / "rankings" / f"sample_ranking_{label}.{ext}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "json":
        df.to_json(output_path, orient="records", indent=2)
    else:
        df.to_csv(output_path, index=False)

    if not args.quiet:
        print(f"Output written to: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
