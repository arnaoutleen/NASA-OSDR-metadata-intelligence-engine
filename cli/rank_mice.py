#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - Mouse Ranking CLI

Generate the Mouse Informativeness table (Table 2) that ranks mice
across all OSDs in a mission by overall data coverage.

Usage:
    python -m cli.rank_mice --mission RR-3
    python -m cli.rank_mice --mission RR-1 -o outputs/rankings/rr1_mice.csv
"""

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
    parser = argparse.ArgumentParser(
        description="NASA OSDR Mouse Ranking - Rank mice by cross-OSD informativeness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
    python -m cli.rank_mice --mission RR-1
    python -m cli.rank_mice --mission RR-3 -o outputs/rankings/rr3_mice.csv
    python -m cli.rank_mice --mission RR-1 --format json
""",
    )

    parser.add_argument(
        "--mission", type=str, required=True,
        help="Mission name (e.g., RR-1, RR-3)",
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

    defaults = get_default_paths()
    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    isa_parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=isa_parser, resolver=resolver)

    mission = args.mission
    osds = resolver.resolve_mission(mission)
    if not osds:
        print(f"Error: No OSDs found for mission '{mission}'", file=sys.stderr)
        return 1

    if not args.quiet:
        print(f"Mission {mission}: {len(osds)} OSDs")

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
    df = scorer.score(records, mission)

    ext = "json" if args.format == "json" else "csv"
    if args.output_path:
        output_path = args.output_path
    else:
        label = mission.replace(" ", "_")
        output_path = defaults["output_dir"] / "rankings" / f"mouse_ranking_{label}.{ext}"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "json":
        df.to_json(output_path, orient="records", indent=2)
    else:
        df.to_csv(output_path, index=False)

    if not args.quiet:
        print(f"\nMouse ranking table: {len(df)} mice")
        print(f"Output written to: {output_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
