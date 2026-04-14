#!/usr/bin/env python3
"""Build or refresh the lightweight OSDR study index cache."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.data_retriever import DataRetriever
from src.core.isa_parser import ISAParser
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.core.study_index import build_study_index, default_index_path
from src.utils.config import get_default_paths


def build_runtime():
    defaults = get_default_paths()
    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=parser, resolver=resolver)
    return client, resolver, retriever


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a cached lightweight study index for organ-first and assay-first discovery.",
    )
    parser.add_argument("--output", type=Path, default=default_index_path(), help="Path to write study_index.json")
    parser.add_argument("--studies", nargs="*", help="Optional subset of OSD IDs to index")
    parser.add_argument("-q", "--quiet", action="store_true", help="Reduce progress output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client, resolver, retriever = build_runtime()
    payload = build_study_index(
        client=client,
        resolver=resolver,
        retriever=retriever,
        output_path=args.output,
        osd_ids=args.studies,
        quiet=args.quiet,
    )
    print(f"Indexed studies: {payload.get('study_count', 0)}")
    print(f"Failed studies: {payload.get('failed_count', 0)}")
    print(f"Study index written to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
