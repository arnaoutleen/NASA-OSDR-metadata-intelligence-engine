#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.data_retriever import DataRetriever
from src.core.finalize_enriched_output import write_all_export_tables
from src.core.informativeness_scorer import InformativenessScorer
from src.core.isa_parser import ISAParser
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient
from src.core.pipeline import Pipeline, PipelineConfig
from src.utils.canonical_schema import records_to_dataframe
from src.utils.config import get_default_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retrieve NASA OSDR records, run metadata enrichment, and optionally rank outputs in one step.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m cli.run_pipeline --project RR-1 -o outputs/rr1
  python -m cli.run_pipeline --mission RR-1 -o outputs/rr1
  python -m cli.run_pipeline --osd OSD-48 -o outputs/osd48
  python -m cli.run_pipeline --osd OSD-48 OSD-102 -o outputs/multi_osd
  python -m cli.run_pipeline --project RR-3 -o outputs/rr3 --skip-ranking
        """,
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--project", type=str, help="Project name, e.g. RR-1")
    target.add_argument("--mission", type=str, help="Alias for --project, kept for backward compatibility")
    target.add_argument("--osd", nargs="+", help="One or more OSD study IDs, e.g. OSD-48 OSD-102")
    parser.add_argument("-o", "--output-dir", type=Path, required=True, help="Directory for pipeline outputs")
    parser.add_argument("--no-cache", action="store_true", help="Do not use API cache during enrichment")
    parser.add_argument("--clear-cache", action="store_true", help="Clear cached OSDR responses before running")
    parser.add_argument("--skip-export-tables", action="store_true", help="Only write retrieved_raw.csv, enriched_native.csv, and provenance outputs")
    parser.add_argument("--skip-ranking", action="store_true", help="Skip sample and mouse informativeness ranking")
    parser.add_argument("-q", "--quiet", action="store_true")
    return parser.parse_args()


def _resolve_targets(args: argparse.Namespace, resolver: MissionResolver) -> tuple[list[str], str, str]:
    project_name = args.project or args.mission or ""

    if project_name:
        osd_ids = resolver.resolve_mission(project_name)
        if not osd_ids:
            raise ValueError(f"No OSD studies found for project '{project_name}'")
        label = project_name.replace(" ", "_")
        return osd_ids, label, project_name

    osd_ids = [OSDRClient.normalize_osd_id(osd_id) for osd_id in args.osd]
    if not osd_ids:
        raise ValueError("No OSD IDs were provided")
    label = osd_ids[0] if len(osd_ids) == 1 else "multi_osd"
    project_name = label
    return osd_ids, label, project_name


def _run_rankings(
    enriched_samples_path: Path,
    output_dir: Path,
    project_name: str,
    quiet: bool = False,
) -> dict[str, Path]:
    df = pd.read_csv(enriched_samples_path)
    records = df.to_dict(orient="records")

    scorer = InformativenessScorer()

    sample_rows = scorer.rank_samples(records, project=project_name)
    sample_path = output_dir / "ranked_samples.csv"
    pd.DataFrame(sample_rows).to_csv(sample_path, index=False)

    mouse_rows = scorer.rank_mice(records, project=project_name)
    mouse_path = output_dir / "ranked_mice.csv"
    pd.DataFrame(mouse_rows).to_csv(mouse_path, index=False)

    if not quiet:
        print(f"Sample ranking written to: {sample_path}")
        print(f"Mouse ranking written to: {mouse_path}")

    return {
        "ranked_samples": sample_path,
        "ranked_mice": mouse_path,
    }


def main() -> int:
    args = parse_args()
    defaults = get_default_paths()
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    client = OSDRClient(cache_dir=defaults["cache_dir"], isa_tab_dir=defaults["isa_tab_dir"])
    if args.clear_cache:
        client.clear_cache()
    parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
    resolver = MissionResolver(client=client, cache_dir=defaults["cache_dir"])
    retriever = DataRetriever(client=client, parser=parser, resolver=resolver)

    try:
        osd_ids, label, project_name = _resolve_targets(args, resolver)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if not args.quiet:
        descriptor = project_name if (args.project or args.mission) else ", ".join(osd_ids)
        print(f"Retrieving records for {descriptor}...")

    records = retriever.retrieve_osds(osd_ids)
    if not records:
        print("Error: retrieval returned 0 sample records", file=sys.stderr)
        return 1

    canonical_df = records_to_dataframe(records, mission=project_name)
    if "mission" in canonical_df.columns and "project" not in canonical_df.columns:
        canonical_df = canonical_df.rename(columns={"mission": "project"})
    elif "mission" in canonical_df.columns and "project" in canonical_df.columns:
        canonical_df["project"] = canonical_df["project"].fillna(canonical_df["mission"])
        canonical_df = canonical_df.drop(columns=["mission"])

    retrieved_csv = output_dir / "retrieved_raw.csv"
    canonical_df.to_csv(retrieved_csv, index=False)

    raw_json = output_dir / "retrieved_raw.json"
    with open(raw_json, "w", encoding="utf-8") as handle:
        json.dump([r.to_dict() for r in records], handle, indent=2)

    pipeline_csv = output_dir / "enriched_native.csv"
    provenance_json = output_dir / "enrichment_provenance.json"
    validation_json = output_dir / "validation_report.json"

    config = PipelineConfig(
        input_csv_path=retrieved_csv,
        output_csv_path=pipeline_csv,
        provenance_log_path=provenance_json,
        validation_report_path=validation_json,
        use_cache=not args.no_cache,
        clear_cache=False,
        fetch_isa_tab=True,
    )
    result = Pipeline(config).run()
    if result.errors:
        for err in result.errors:
            print(f"Pipeline error: {err}", file=sys.stderr)
        return 1

    enriched_samples_path = output_dir / "enriched_samples.csv"
    if not args.skip_export_tables:
        outputs = write_all_export_tables(
            retrieved_csv_path=retrieved_csv,
            pipeline_csv_path=pipeline_csv,
            provenance_json_path=provenance_json,
            output_dir=output_dir,
        )
        enriched_samples_path = outputs["enriched_samples"]
        if not args.quiet:
            print(f"Rich sample export written to: {outputs['enriched_samples']}")
            print(f"Mouse-level table written to: {outputs['mouse_metadata']}")
            print(f"Sample-level table written to: {outputs['sample_metadata']}")
            print(f"Assay-parameter table written to: {outputs['assay_parameters_long']}")
    else:
        # If export tables are skipped, fall back to ranking native pipeline output.
        enriched_samples_path = pipeline_csv

    if not args.skip_ranking:
        try:
            _run_rankings(
                enriched_samples_path=enriched_samples_path,
                output_dir=output_dir,
                project_name=project_name,
                quiet=args.quiet,
            )
        except Exception as exc:
            print(f"Ranking error: {exc}", file=sys.stderr)
            return 1

    if not args.quiet:
        print(f"Retrieved {len(records)} samples across {len(osd_ids)} OSD studies")
        print(f"Intermediate native enrichment: {pipeline_csv}")
        print(f"Provenance log: {provenance_json}")
        print(f"Label: {label}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
