#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - Combined Ranking CLI

Generate both the Sample Informativeness table and the Mouse
Informativeness table for a project or all known projects.
Pooled samples are excluded from both tables.
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.data_retriever import DataRetriever
from src.core.informativeness_scorer import (
    MouseInformativenessScorer,
    SampleInformativenessScorer,
)
from src.core.isa_parser import ISAParser
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.utils.config import get_default_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="NASA OSDR Combined Ranking - Generate sample and mouse tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
    python -m cli.rank_all --project RR-3 -o outputs/rankings/
    python -m cli.rank_all --all -o outputs/rankings/
    python -m cli.rank_all --mission RR-1 --format json
""",
    )

    parser.add_argument(
        "--project", "--mission", dest="project", type=str, default=None,
        help="Project name (e.g., RR-1, RR-3)",
    )
    parser.add_argument(
        "--all", action="store_true", dest="rank_all",
        help="Generate tables for all known projects",
    )
    parser.add_argument(
        "-o", "--output", type=Path, dest="output_dir",
        help="Output directory (default: outputs/rankings/)",
    )
    parser.add_argument(
        "--format", choices=["csv", "json"], default="csv",
        help="Output format (default: csv)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-q", "--quiet", action="store_true")

    return parser.parse_args()


def _rank_project(
    project: str,
    retriever: DataRetriever,
    resolver: MissionResolver,
    output_dir: Path,
    fmt: str,
    quiet: bool,
) -> bool:
    """Generate both tables for a single project. Returns True on success."""
    osds = resolver.resolve_mission(project)
    if not osds:
        if not quiet:
            print(f"  No OSDs found for project '{project}' -- skipping")
        return False

    if not quiet:
        print(f"\nProject {project}: {len(osds)} OSDs")

    records = []
    for i, osd in enumerate(osds, 1):
        if not quiet:
            print(f"  [{i}/{len(osds)}] {osd}...", end=" ", flush=True)
        try:
            recs = retriever.retrieve_osd(osd)
            records.extend(recs)
            if not quiet:
                print(f"({len(recs)} samples)")
        except Exception as e:
            if not quiet:
                print(f"(error: {e})")

    if not records:
        if not quiet:
            print(f"  No records for {project}")
        return False

    label = project.replace(" ", "_")
    ext = "json" if fmt == "json" else "csv"
    output_dir.mkdir(parents=True, exist_ok=True)

    sample_scorer = SampleInformativenessScorer()
    sample_df = sample_scorer.score(records, project=project)
    sample_path = output_dir / f"sample_ranking_{label}.{ext}"
    if fmt == "json":
        sample_df.to_json(sample_path, orient="records", indent=2)
    else:
        sample_df.to_csv(sample_path, index=False)
    if not quiet:
        print(f"  Sample table: {len(sample_df)} rows -> {sample_path}")

    mouse_scorer = MouseInformativenessScorer()
    mouse_df = mouse_scorer.score(records, project)
    mouse_path = output_dir / f"mouse_ranking_{label}.{ext}"
    if fmt == "json":
        mouse_df.to_json(mouse_path, orient="records", indent=2)
    else:
        mouse_df.to_csv(mouse_path, index=False)
    if not quiet:
        print(f"  Mouse table: {len(mouse_df)} rows -> {mouse_path}")

    return True


def main() -> int:
    args = parse_args()

    if not args.project and not args.rank_all:
        print("Error: Provide --project or --all", file=sys.stderr)
        return 1

    defaults = get_default_paths()
    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    isa_parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=isa_parser, resolver=resolver)

    output_dir = args.output_dir or (defaults["output_dir"] / "rankings")

    if args.rank_all:
        projects = resolver.list_known_missions()
        if not args.quiet:
            print(f"Generating tables for {len(projects)} projects...")
        success = 0
        for project in projects:
            if _rank_project(project, retriever, resolver, output_dir, args.format, args.quiet):
                success += 1
        if not args.quiet:
            print(f"\nCompleted {success}/{len(projects)} projects")
        return 0 if success > 0 else 1

    return 0 if _rank_project(args.project, retriever, resolver, output_dir, args.format, args.quiet) else 1


if __name__ == "__main__":
    sys.exit(main())
