#!/usr/bin/env python3
"""Build or refresh the local OSDR study index cache."""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.core.study_index import StudyIndexCache
from src.utils.config import get_default_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a cached study index for organ/assay-first OSDR discovery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
    python -m cli.build_study_index
    python -m cli.build_study_index --output resources/osdr_api/study_index.json
    python -m cli.build_study_index --force
""",
    )
    parser.add_argument("--output", type=Path, default=None, help="Path to write the study index JSON")
    parser.add_argument("--force", action="store_true", help="Rebuild even if the index already exists")
    parser.add_argument("-q", "--quiet", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    defaults = get_default_paths()
    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    index_path = args.output or (defaults["cache_dir"].parent / "study_index.json")
    cache = StudyIndexCache(client=client, resolver=resolver, index_path=index_path)

    if index_path.exists() and not args.force:
        index = cache.load()
        if index.get("studies"):
            if not args.quiet:
                print(f"Study index already exists at {index_path} with {index.get('study_count', 0)} studies")
                print("Use --force to rebuild it.")
            return 0

    index = cache.build(verbose=not args.quiet)
    cache.save(index)
    if not args.quiet:
        print(f"Built study index with {index.get('study_count', 0)} studies")
        print(f"Output written to: {index_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
