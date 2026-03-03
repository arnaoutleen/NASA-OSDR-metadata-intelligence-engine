#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - Data Retrieval CLI

Retrieve unified sample records from NASA OSDR for specific OSD studies,
an entire mission, or all known studies.

Usage:
    python -m cli.retrieve_data OSD-242 OSD-379
    python -m cli.retrieve_data --mission RR-3
    python -m cli.retrieve_data --all
"""

import argparse
import csv
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.data_retriever import DataRetriever, SampleRecord
from src.core.isa_parser import ISAParser
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.utils.config import get_default_paths

OUTPUT_COLUMNS = [
    "osd", "source_name", "sample_name", "organism", "material_type",
    "measurement_types", "technology_types", "device_platforms", "data_files",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="NASA OSDR Data Retrieval - Fetch unified sample records",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
    python -m cli.retrieve_data OSD-242 OSD-379
    python -m cli.retrieve_data --mission RR-3
    python -m cli.retrieve_data --all -o outputs/retrieval/all_data.csv
    python -m cli.retrieve_data OSD-102 --format json
""",
    )

    parser.add_argument(
        "osd_ids", nargs="*", default=[],
        help="OSD study IDs to retrieve (e.g., OSD-242 OSD-379)",
    )
    parser.add_argument(
        "--mission", type=str, default=None,
        help="Retrieve all OSDs for a mission (e.g., RR-3)",
    )
    parser.add_argument(
        "--all", action="store_true", dest="retrieve_all",
        help="Retrieve all known studies",
    )
    parser.add_argument(
        "-o", "--output", type=Path, dest="output_path",
        help="Output file path (default: outputs/retrieval/<name>_data.<ext>)",
    )
    parser.add_argument(
        "--format", choices=["csv", "json"], default="csv",
        help="Output format (default: csv)",
    )
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--clear-cache", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-q", "--quiet", action="store_true")

    return parser.parse_args()


def _build_components(args):
    defaults = get_default_paths()
    client = OSDRClient(
        cache_dir=defaults["cache_dir"],
        isa_tab_dir=defaults["isa_tab_dir"],
    )
    if args.clear_cache:
        client.clear_cache()

    parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=parser, resolver=resolver)
    return retriever, resolver


def _record_to_row(r: SampleRecord) -> dict:
    return {
        "osd": r.osd,
        "source_name": r.source_name,
        "sample_name": r.sample_name,
        "organism": r.organism,
        "material_type": r.material_type,
        "measurement_types": " | ".join(r.measurement_types),
        "technology_types": " | ".join(r.technology_types),
        "device_platforms": " | ".join(r.device_platforms),
        "data_files": " | ".join(r.data_files),
    }


def _write_output(records, output_path, fmt, quiet):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "json":
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump([r.to_dict() for r in records], f, indent=2)
    else:
        rows = [_record_to_row(r) for r in records]
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    if not quiet:
        print(f"Output written to: {output_path}")


def main() -> int:
    args = parse_args()

    if not args.osd_ids and not args.mission and not args.retrieve_all:
        print("Error: Provide OSD IDs, --mission, or --all", file=sys.stderr)
        return 1

    retriever, resolver = _build_components(args)
    records: list[SampleRecord] = []

    if args.retrieve_all:
        if not args.quiet:
            print("Retrieving all known studies...")
        missions = resolver.discover_all_missions()
        all_osds: list[str] = []
        for osds in missions.values():
            all_osds.extend(osds)
        all_osds = sorted(set(all_osds), key=lambda x: int(x.replace("OSD-", "")))
        for i, osd in enumerate(all_osds, 1):
            if not args.quiet:
                print(f"  [{i}/{len(all_osds)}] {osd}...", end=" ", flush=True)
            try:
                recs = retriever.retrieve_osd(osd)
                records.extend(recs)
                if not args.quiet:
                    print(f"({len(recs)} samples)")
            except Exception as e:
                if not args.quiet:
                    print(f"(error: {e})")
        label = "all_missions"

    elif args.mission:
        mission = args.mission
        osds = resolver.resolve_mission(mission)
        if not osds:
            print(f"Error: No OSDs found for mission '{mission}'", file=sys.stderr)
            return 1
        if not args.quiet:
            print(f"Mission {mission}: {len(osds)} OSDs")
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
        label = mission.replace(" ", "_")

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

    # Determine output path
    ext = "json" if args.format == "json" else "csv"
    if args.output_path:
        output_path = args.output_path
    else:
        defaults = get_default_paths()
        output_path = defaults["output_dir"] / "retrieval" / f"{label}_data.{ext}"

    _write_output(records, output_path, args.format, args.quiet)

    if not args.quiet:
        print(f"\nTotal samples retrieved: {len(records)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
