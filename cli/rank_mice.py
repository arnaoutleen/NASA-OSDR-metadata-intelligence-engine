#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.data_retriever import DataRetriever
from src.core.informativeness_scorer import MouseInformativenessScorer
from src.core.isa_parser import ISAParser
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.utils.config import get_default_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank mice/source rows by per-OSD informativeness")
    parser.add_argument("--project", type=str, help="Project name, e.g. RR-3")
    parser.add_argument("--mission", type=str, help="Alias for --project", default=None)
    parser.add_argument("-o", "--output", type=Path, dest="output_path")
    parser.add_argument("--format", choices=["csv", "json"], default="csv")
    parser.add_argument("-q", "--quiet", action="store_true")
    args = parser.parse_args()
    if not args.project and not args.mission:
        parser.error("Provide --project (or --mission)")
    return args


def main() -> int:
    args = parse_args()
    project = args.project or args.mission

    defaults = get_default_paths()
    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    isa_parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=isa_parser, resolver=resolver)

    osds = resolver.resolve_mission(project)
    if not osds:
        print(f"Error: No OSDs found for project '{project}'", file=sys.stderr)
        return 1

    records = []
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

    scorer = MouseInformativenessScorer()
    df = scorer.score(records, project=project)

    ext = "json" if args.format == "json" else "csv"
    output_path = args.output_path or (defaults["output_dir"] / "rankings" / f"mouse_ranking_{project.replace(' ', '_')}.{ext}")
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
