#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - Sample Ranking CLI

Generate the Sample Informativeness table that ranks samples within OSDs
by number of assay types and data files available. Pooled samples are removed.

Usage:
    python -m cli.rank_samples OSD-242 OSD-379
    python -m cli.rank_samples --project RR-3
"""

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
    parser = argparse.ArgumentParser(
        description="NASA OSDR Sample Ranking - Rank samples by data availability",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
    python -m cli.rank_samples OSD-242 OSD-379
    python -m cli.rank_samples --project RR-3 -o outputs/rankings/rr3_samples.csv
    python -m cli.rank_samples --mission RR-1 --format json
""",
    )

    parser.add_argument(
        "osd_ids", nargs="*", default=[],
        help="OSD study IDs to rank (e.g., OSD-242 OSD-379)",
    )
    parser.add_argument(
        "--project", "--mission", dest="project", type=str, default=None,
        help="Rank samples for all OSDs in a project",
    )
    parser.add_argument(
        "-o", "--output", type=Path, dest="output_path",
        help="Output file path",
    )
    parser.add_argument(
        "--format", choices=["csv", "json"], default="csv",
        help="Output format (default: csv)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-q", "--quiet", action="store_true")

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.osd_ids and not args.project:
        print("Error: Provide OSD IDs or --project", file=sys.stderr)
        return 1

    defaults = get_default_paths()
    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=parser, resolver=resolver)

    records = []
    if args.project:
        osds = resolver.resolve_mission(args.project)
        if not osds:
            print(f"Error: No OSDs found for project '{args.project}'", file=sys.stderr)
            return 1
        if not args.quiet:
            print(f"Project {args.project}: {len(osds)} OSDs")
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
        label = args.project.replace(" ", "_")
        project = args.project
    else:
        for i, osd in enumerate(args.osd_ids, 1):
            if not args.quiet:
                print(f"  [{i}/{len(args.osd_ids)}] {osd}...", end=" ", flush=True)
            try:
                recs = retriever.retrieve_osd(osd)
                records.extend(recs)
                if not args.quiet:
                    print(f"({len(recs)} samples)")
            except Exception as e:
                if not args.quiet:
                    print(f"(error: {e})")
        label = "_".join(args.osd_ids) if len(args.osd_ids) <= 3 else "multi"
        project = None

    if not records:
        print("Error: No sample records retrieved", file=sys.stderr)
        return 1

    scorer = SampleInformativenessScorer()
    df = scorer.score(records, project=project)

    ext = "json" if args.format == "json" else "csv"
    if args.output_path:
        output_path = args.output_path
    else:
        output_path = defaults["output_dir"] / "rankings" / f"sample_ranking_{label}.{ext}"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "json":
        df.to_json(output_path, orient="records", indent=2)
    else:
        df.to_csv(output_path, index=False)

    if not args.quiet:
        print(f"\nSample ranking table: {len(df)} rows")
        print(f"Output written to: {output_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
