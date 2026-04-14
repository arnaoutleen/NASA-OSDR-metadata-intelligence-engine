#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.data_retriever import DataRetriever
from src.core.informativeness_scorer import MouseInformativenessScorer, SampleInformativenessScorer
from src.core.isa_parser import ISAParser
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.utils.config import get_default_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate sample and mouse ranking tables")
    parser.add_argument("--project", type=str, default=None)
    parser.add_argument("--mission", type=str, default=None, help="Alias for --project")
    parser.add_argument("--all", action="store_true", dest="rank_all")
    parser.add_argument("-o", "--output", type=Path, dest="output_dir")
    parser.add_argument("--format", choices=["csv", "json"], default="csv")
    parser.add_argument("-q", "--quiet", action="store_true")
    args = parser.parse_args()
    if not args.rank_all and not args.project and not args.mission:
        parser.error("Provide --project (or --mission) or --all")
    return args


def _rank_project(project: str, retriever: DataRetriever, resolver: MissionResolver, output_dir: Path, fmt: str, quiet: bool) -> bool:
    osds = resolver.resolve_mission(project)
    if not osds:
        if not quiet:
            print(f"No OSDs found for project '{project}'")
        return False

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
        return False

    label = project.replace(" ", "_")
    ext = "json" if fmt == "json" else "csv"
    output_dir.mkdir(parents=True, exist_ok=True)

    sample_df = SampleInformativenessScorer().score(records, project=project)
    mouse_df = MouseInformativenessScorer().score(records, project=project)

    sample_path = output_dir / f"sample_ranking_{label}.{ext}"
    mouse_path = output_dir / f"mouse_ranking_{label}.{ext}"

    if fmt == "json":
        sample_df.to_json(sample_path, orient="records", indent=2)
        mouse_df.to_json(mouse_path, orient="records", indent=2)
    else:
        sample_df.to_csv(sample_path, index=False)
        mouse_df.to_csv(mouse_path, index=False)

    if not quiet:
        print(f"Sample ranking -> {sample_path}")
        print(f"Mouse ranking -> {mouse_path}")
    return True


def main() -> int:
    args = parse_args()
    defaults = get_default_paths()
    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    isa_parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=isa_parser, resolver=resolver)
    output_dir = args.output_dir or (defaults["output_dir"] / "rankings")

    if args.rank_all:
        projects = resolver.list_known_missions()
        ok_count = 0
        for project in projects:
            if _rank_project(project, retriever, resolver, output_dir, args.format, args.quiet):
                ok_count += 1
        if not args.quiet:
            print(f"Completed: {ok_count}/{len(projects)} projects")
        return 0

    project = args.project or args.mission
    return 0 if _rank_project(project, retriever, resolver, output_dir, args.format, args.quiet) else 1


if __name__ == "__main__":
    sys.exit(main())
