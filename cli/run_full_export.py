#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - Full Export CLI

Pulls ISA-Tab for one or more OSD studies, expands to sample-level rows via
SampleExpander, then writes three clean output tables:

    <out_dir>/mouse_metadata.csv       — one row per animal
    <out_dir>/sample_metadata.csv      — one row per sample × assay
    <out_dir>/assay_parameters_long.csv  — long-form parameter table
    <out_dir>/assay_parameters_wide.csv  — wide-form (one param per column)

Usage
-----
    # Single study
    python -m cli.run_full_export --osd OSD-48 -o outputs/test_osd48

    # Multiple studies
    python -m cli.run_full_export --osd OSD-48 OSD-87 OSD-168 -o outputs/rr1

    # From a CSV that has an OSD_study column (and optionally RR_mission)
    python -m cli.run_full_export --input resources/test_inputs/demo/realworld_rodent_research.csv
"""

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from src.core.sample_expander import SampleExpander
from src.core.export_tables import build_export_tables
from src.utils.assay_wide import assay_long_to_wide
from src.utils.config import get_default_paths


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_osd_list_from_csv(
    csv_path: Path,
    osd_col: str = "OSD_study",
    mission_col: str = "RR_mission",
) -> list[tuple[str, str]]:
    """Return [(osd_id, mission), ...] from an input CSV."""
    pairs = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            osd = row.get(osd_col, "").strip()
            mission = row.get(mission_col, "").strip()
            if osd:
                pairs.append((osd, mission))
    return pairs


def _samplerow_to_record(row_dict: dict, osd_id: str, mission: str) -> dict:
    """
    Enrich a SampleRow.to_dict() with the keys that export_tables expects.

    SampleRow uses flat field names (mouse_uid, OSD_study, …).
    export_tables._safe_get() already has fallbacks for most of them,
    but we add a few canonical aliases here to be explicit.
    """
    row_dict.setdefault("osd_id", osd_id)
    row_dict.setdefault("OSD_study", osd_id)
    row_dict.setdefault("mission", mission or osd_id)
    row_dict.setdefault("RR_mission", mission or osd_id)
    # mouse_uid is already the source_name / mouse identifier
    row_dict.setdefault("mouse_id", row_dict.get("mouse_uid", ""))
    row_dict.setdefault("sample_id", row_dict.get("sample_name", ""))
    return row_dict


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_export(
    osd_pairs: list[tuple[str, str]],
    out_dir: Path,
    cache_dir: Path | None = None,
    isa_tab_dir: Path | None = None,
) -> None:
    defaults = get_default_paths()
    expander = SampleExpander(
        cache_dir=cache_dir or defaults["cache_dir"],
        isa_tab_dir=isa_tab_dir or defaults["isa_tab_dir"],
    )

    records: list[dict] = []

    for osd_id, mission in osd_pairs:
        print(f"  Expanding {osd_id} ...", end=" ", flush=True)
        sample_rows = expander.expand_osd_to_samples(osd_id, rr_mission=mission)
        if not sample_rows:
            print("no samples found — skipped")
            continue
        print(f"{len(sample_rows)} samples")

        for sr in sample_rows:
            rec = _samplerow_to_record(sr.to_dict(), osd_id, mission)
            records.append(rec)

    if not records:
        print("No records to export.")
        return

    print(f"\nBuilding export tables from {len(records)} records …")
    mouse_df, sample_df, assay_long_df = build_export_tables(records)

    out_dir.mkdir(parents=True, exist_ok=True)

    mouse_path = out_dir / "mouse_metadata.csv"
    sample_path = out_dir / "sample_metadata.csv"
    assay_long_path = out_dir / "assay_parameters_long.csv"
    assay_wide_path = out_dir / "assay_parameters_wide.csv"

    mouse_df.to_csv(mouse_path, index=False)
    sample_df.to_csv(sample_path, index=False)
    assay_long_df.to_csv(assay_long_path, index=False)

    # Wide pivot: one column per parameter, rows = (osd_id, sample_id, assay_category)
    if not assay_long_df.empty:
        try:
            assay_wide_df = assay_long_to_wide(assay_long_df)
            assay_wide_df.to_csv(assay_wide_path, index=False)
        except Exception as exc:
            print(f"  Warning: could not build wide table — {exc}")
            assay_wide_df = pd.DataFrame()
            assay_wide_df.to_csv(assay_wide_path, index=False)
    else:
        pd.DataFrame().to_csv(assay_wide_path, index=False)

    # Summary
    print("\n" + "=" * 60)
    print("EXPORT COMPLETE")
    print("=" * 60)
    print(f"  Mice:           {len(mouse_df)} rows  →  {mouse_path}")
    print(f"  Samples:        {len(sample_df)} rows  →  {sample_path}")
    print(f"  Assay params:   {len(assay_long_df)} rows  →  {assay_long_path}")
    if not assay_long_df.empty:
        print(f"  Assay wide:     {assay_wide_path}")

    # Quick fill-rate report for the fields you care most about
    print()
    print("Fill-rate check (mouse_metadata):")
    key_mouse = ["strain", "sex", "age", "spaceflight_status", "duration",
                 "habitat", "animal_source", "genotype"]
    for col in key_mouse:
        if col in mouse_df.columns:
            n = mouse_df[col].notna().sum() - (mouse_df[col] == "").sum()
            print(f"  {col:25s}: {n}/{len(mouse_df)}")

    print()
    print("Fill-rate check (sample_metadata):")
    key_sample = ["mouse_strain", "mouse_sex", "age", "space_or_ground",
                  "assay_category", "material_type"]
    for col in key_sample:
        if col in sample_df.columns:
            n = sample_df[col].notna().sum() - (sample_df[col] == "").sum()
            print(f"  {col:25s}: {n}/{len(sample_df)}")

    if not assay_long_df.empty:
        print()
        print("Assay parameter coverage:")
        for cat, grp in assay_long_df.groupby("assay_category"):
            params = grp["parameter_name"].nunique()
            filled = grp["parameter_value"].notna().sum()
            print(f"  {cat}: {params} distinct params, {filled} filled values")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="NASA OSDR Full Export — mouse + sample + assay parameter tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--osd", nargs="+", metavar="OSD_ID",
                   help="One or more OSD study IDs (e.g. OSD-48 OSD-87)")
    p.add_argument("--mission", default="",
                   help="Mission label for all --osd studies (e.g. RR-1)")
    p.add_argument("--input", type=Path, metavar="CSV",
                   help="CSV with OSD_study (and optionally RR_mission) column")
    p.add_argument("--osd-col", default="OSD_study",
                   help="Column name for OSD IDs in --input CSV (default: OSD_study)")
    p.add_argument("--mission-col", default="RR_mission",
                   help="Column name for mission in --input CSV (default: RR_mission)")
    p.add_argument("-o", "--output", type=Path, default=Path("outputs/full_export"),
                   help="Output directory (default: outputs/full_export)")
    p.add_argument("--cache-dir", type=Path)
    p.add_argument("--isa-tab-dir", type=Path)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    osd_pairs: list[tuple[str, str]] = []

    if args.input:
        osd_pairs = _read_osd_list_from_csv(args.input, args.osd_col, args.mission_col)
        print(f"Loaded {len(osd_pairs)} OSD IDs from {args.input}")
    elif args.osd:
        osd_pairs = [(osd, args.mission) for osd in args.osd]
    else:
        print("Error: provide --osd OSD-48 [OSD-87 …] or --input <csv>", file=sys.stderr)
        return 1

    run_export(
        osd_pairs=osd_pairs,
        out_dir=args.output,
        cache_dir=args.cache_dir,
        isa_tab_dir=args.isa_tab_dir,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
