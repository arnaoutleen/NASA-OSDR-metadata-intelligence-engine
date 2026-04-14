#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - Full Export CLI

Pulls ISA-Tab for one or more OSD studies, expands to sample-level rows via
SampleExpander, then writes four clean output tables:

    <out_dir>/mouse_metadata.csv         — one row per animal
    <out_dir>/sample_metadata.csv        — one row per sample x assay
    <out_dir>/assay_parameters_long.csv  — long-form parameter table
    <out_dir>/assay_parameters_wide.csv  — wide-form (one param per column)
    <out_dir>/data_manifest.json         — per-study pull timestamps + ISA-Tab hashes

All output tables include a `pulled_at` column (UTC ISO-8601) recording when
the OSDR data was fetched.  On subsequent runs the tool hashes the ISA-Tab files
for each study and compares them against the stored manifest:
  - If the files are unchanged the original timestamp is kept.
  - If anything changed the study is re-expanded and the timestamp is updated.

Usage
-----
    # Single study
    python -m cli.run_full_export --osd OSD-48

    # Multiple studies
    python -m cli.run_full_export --osd OSD-48 OSD-87 OSD-168

    # Entire mission (resolves OSD IDs via live API)
    python -m cli.run_full_export --mission RR-1

    # From a CSV with OSD_study (and optionally RR_mission) columns
    python -m cli.run_full_export --input resources/test_inputs/demo/realworld_rodent_research.csv

    # Custom output directory (default: outputs/full_export)
    python -m cli.run_full_export --mission RR-1 -o outputs/rr1
"""

import argparse
import csv
import gc
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from src.core.sample_expander import SampleExpander
from src.core.export_tables import build_export_tables
from src.core.mission_resolver import MissionResolver, normalize_mission_name
from src.core.osdr_client import OSDRClient
from src.utils.assay_wide import assay_long_to_wide
from src.core.informativeness_scorer import MouseRankerFromExport
from src.utils.config import get_default_paths


# ---------------------------------------------------------------------------
# ISA-Tab hashing — the ground truth for "did the data change?"
# ---------------------------------------------------------------------------

def _hash_isa_tab_dir(isa_tab_dir: Path, osd_id: str) -> str:
    """
    Return a single hex digest representing the content of all ISA-Tab files
    for a study.  Files are sorted by name so the hash is stable across runs.
    Returns an empty string if the directory doesn't exist.
    """
    study_dir = isa_tab_dir / osd_id
    if not study_dir.exists():
        return ""

    h = hashlib.sha256()
    for fpath in sorted(study_dir.iterdir()):
        if fpath.is_file():
            h.update(fpath.name.encode())          # include filename
            h.update(fpath.read_bytes())            # include content
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Manifest — stores per-study hash + timestamp in <out_dir>/data_manifest.json
# ---------------------------------------------------------------------------

MANIFEST_FILE = "data_manifest.json"


def _load_manifest(out_dir: Path) -> dict:
    """Load the manifest from a previous run, or return an empty dict."""
    p = out_dir / MANIFEST_FILE
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def _save_manifest(out_dir: Path, manifest: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / MANIFEST_FILE).write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Per-study change detection
# ---------------------------------------------------------------------------

def _check_study(
    osd_id: str,
    isa_tab_dir: Path,
    manifest: dict,
) -> tuple[bool, str]:
    """
    Compare the current ISA-Tab hash for osd_id against the stored manifest.

    Returns
    -------
    changed : bool
        True  → data changed or never seen before (re-expand needed)
        False → data identical to previous run (keep old timestamp)
    pulled_at : str
        The timestamp to record: old one if unchanged, new UTC now if changed.
    """
    current_hash = _hash_isa_tab_dir(isa_tab_dir, osd_id)
    entry = manifest.get(osd_id, {})
    stored_hash = entry.get("isa_hash", "")
    stored_ts = entry.get("pulled_at", "")

    if current_hash and current_hash == stored_hash and stored_ts:
        return False, stored_ts   # unchanged — keep original timestamp

    return True, _now_utc()       # new or changed


# ---------------------------------------------------------------------------
# Live API verification (unchanged from previous version)
# ---------------------------------------------------------------------------

def _verify_osd_exists(osd_id: str, client: OSDRClient) -> bool:
    """
    Lightweight existence check — only fetches study-level metadata (no samples).

    Uses the dataset endpoint (/v2/dataset/{OSD-ID}/) which returns a small
    JSON object with title/description only. This avoids the expensive wildcard
    samples endpoint (/assay/*/sample/*/) that can return hundreds of KB per
    study and cause OOM kills when checking 10+ studies in sequence.

    Falls back to checking the local ISA-Tab cache, which is free.
    """
    osd_id = OSDRClient.normalize_osd_id(osd_id)

    # Free check: ISA-Tab already downloaded?
    isa_dir = client.isa_tab_dir / osd_id
    if isa_dir.exists() and any(isa_dir.glob("s_*.txt")):
        return True

    # Free check: JSON cache already exists?
    cache_path = client.cache_dir / f"{osd_id}.json"
    if cache_path.exists():
        return True

    # Lightweight API check: dataset endpoint only (no samples payload)
    data = client._fetch_biodata_dataset(osd_id)
    if data:
        return True

    # Last resort: developer API metadata (also lightweight)
    dev = client._fetch_developer_metadata(osd_id)
    return dev is not None


def _discover_mission_osds(
    mission_name: str,
    client: OSDRClient,
    resolver: MissionResolver,
) -> list[tuple[str, str]]:
    canonical = normalize_mission_name(mission_name)

    registry_osds = resolver.resolve_mission(canonical)
    if registry_osds:
        print(f"  Found {len(registry_osds)} studies for '{canonical}' in registry.")
        print(f"  Skipping per-OSD API verification for registry missions "
              f"(use --osd to verify individual studies).")
        return [(osd_id, mission_name) for osd_id in registry_osds]

    print(f"  Mission '{canonical}' not in local registry — scanning live API ...")
    all_ids = client.list_all_study_ids()
    if not all_ids:
        print("  Error: could not retrieve study list from API.", file=sys.stderr)
        return []

    print(f"  API reports {len(all_ids)} total studies. Scanning for '{canonical}' ...")
    matched = []
    for osd_id in all_ids:
        found_mission = resolver.get_mission_for_osd(osd_id)
        if found_mission and normalize_mission_name(found_mission) == canonical:
            matched.append(osd_id)

    return [(osd_id, mission_name) for osd_id in matched]


def _resolve_input_osds(osd_ids: list[str], client: OSDRClient) -> list[str]:
    valid = []
    for raw_id in osd_ids:
        osd_id = OSDRClient.normalize_osd_id(raw_id)
        print(f"  Verifying {osd_id} ...", end=" ", flush=True)
        if _verify_osd_exists(osd_id, client):
            print("OK")
            valid.append(osd_id)
        else:
            print(f"NOT FOUND — '{osd_id}' does not exist or is not reachable. Skipping.")
    return valid


# ---------------------------------------------------------------------------
# CSV input helper
# ---------------------------------------------------------------------------

def _read_osd_list_from_csv(
    csv_path: Path,
    osd_col: str = "OSD_study",
    mission_col: str = "RR_mission",
) -> list[tuple[str, str]]:
    pairs = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            osd = row.get(osd_col, "").strip()
            mission = row.get(mission_col, "").strip()
            if osd:
                pairs.append((osd, mission))
    return pairs


# ---------------------------------------------------------------------------
# Record bridge
# ---------------------------------------------------------------------------

def _samplerow_to_record(row_dict: dict, osd_id: str, mission: str, pulled_at: str) -> dict:
    """
    Add canonical key aliases that export_tables expects, plus the pull timestamp.

    payload  (internal key) = Comment[Mission Name], e.g. "SpaceX-4"
               → exported as "mission" column (rocket/vehicle name).
    mission  (internal key) = RR project identifier, e.g. "RR-1"
               → exported as "project" column.
    pulled_at = UTC timestamp of when this study's data was fetched from OSDR.
    """
    row_dict.setdefault("osd_id", osd_id)
    row_dict.setdefault("OSD_study", osd_id)
    row_dict.setdefault("mission", mission or row_dict.get("RR_mission", osd_id))
    row_dict.setdefault("RR_mission", row_dict.get("mission", mission or osd_id))
    row_dict.setdefault("payload", row_dict.get("payload", ""))
    row_dict.setdefault("mouse_id", row_dict.get("mouse_uid", ""))
    row_dict.setdefault("sample_id", row_dict.get("sample_name", ""))
    row_dict["pulled_at"] = pulled_at   # always set — not just a default
    return row_dict


# ---------------------------------------------------------------------------
# Core export logic
# ---------------------------------------------------------------------------

def run_export(
    osd_pairs: list[tuple[str, str]],
    out_dir: Path,
    cache_dir: Path | None = None,
    isa_tab_dir: Path | None = None,
    include_assays: list[str] | None = None,
    exclude_assays: list[str] | None = None,
) -> None:
    defaults = get_default_paths()
    cache_dir   = cache_dir   or defaults["cache_dir"]
    isa_tab_dir = isa_tab_dir or defaults["isa_tab_dir"]

    if include_assays:
        print(f"Assay filter — including only: {include_assays}")
    if exclude_assays:
        print(f"Assay filter — excluding: {exclude_assays}")

    expander = SampleExpander(
        cache_dir=cache_dir,
        isa_tab_dir=isa_tab_dir,
        include_assays=include_assays,
        exclude_assays=exclude_assays,
    )

    # Load manifest from a previous run (if any)
    manifest = _load_manifest(out_dir)
    updated_manifest: dict = {}

    records: list[dict] = []
    study_status: list[tuple[str, str, bool]] = []   # (osd_id, pulled_at, changed)

    for osd_id, mission in osd_pairs:
        # ── Expand ISA-Tab first (downloads if not cached) ──────────────────
        print(f"  Expanding {osd_id} ...", end=" ", flush=True)
        sample_rows = expander.expand_osd_to_samples(osd_id, rr_mission=mission)
        if not sample_rows:
            print("no samples found — skipped")
            continue
        print(f"{len(sample_rows)} samples", end="")

        # ── Compare ISA-Tab hash against stored manifest ─────────────────────
        changed, pulled_at = _check_study(osd_id, isa_tab_dir, manifest)
        if changed:
            print(f"  [NEW/UPDATED — {pulled_at}]")
        else:
            print(f"  [unchanged — keeping {pulled_at}]")

        # ── Record the hash + timestamp for this study ───────────────────────
        updated_manifest[osd_id] = {
            "pulled_at":  pulled_at,
            "isa_hash":   _hash_isa_tab_dir(isa_tab_dir, osd_id),
            "n_samples":  len(sample_rows),
            "changed":    changed,
        }
        study_status.append((osd_id, pulled_at, changed))

        for sr in sample_rows:
            rec = _samplerow_to_record(sr.to_dict(), osd_id, mission, pulled_at)
            records.append(rec)

        # Explicitly free the study's SampleRow objects after converting to dicts.
        # This prevents accumulation of large ISA-Tab-derived objects in RAM
        # when processing many studies in sequence.
        del sample_rows
        gc.collect()

    if not records:
        print("No records to export.")
        return

    print(f"\nBuilding export tables from {len(records)} records …")
    _placeholder_mouse_df, sample_df, assay_long_df = build_export_tables(records)

    # ── Mouse metadata = ranker output (one row per mouse, cross-OSD) ────────
    out_dir.mkdir(parents=True, exist_ok=True)
    mouse_path      = out_dir / "mouse_metadata.csv"
    sample_path     = out_dir / "sample_metadata.csv"
    assay_long_path = out_dir / "assay_parameters_long.csv"
    assay_wide_path = out_dir / "assay_parameters_wide.csv"

    sample_df.to_csv(sample_path, index=False)
    assay_long_df.to_csv(assay_long_path, index=False)

    if not assay_long_df.empty:
        try:
            assay_long_to_wide(assay_long_df).to_csv(assay_wide_path, index=False)
        except Exception as exc:
            print(f"  Warning: could not build wide table — {exc}")
            pd.DataFrame().to_csv(assay_wide_path, index=False)
    else:
        pd.DataFrame().to_csv(assay_wide_path, index=False)

    # ── Save manifest ────────────────────────────────────────────────────────
    _save_manifest(out_dir, updated_manifest)

    # ── Mouse metadata = ranker output ───────────────────────────────────────
    # One row per mouse, aggregated across all OSDs.
    try:
        ranker = MouseRankerFromExport()
        ranking_df = ranker.score(sample_df)
        ranking_df.to_csv(mouse_path, index=False)
    except Exception as exc:
        print(f"  Warning: mouse ranking failed — {exc}")
        ranking_df = pd.DataFrame()

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("EXPORT COMPLETE")
    print("=" * 60)
    print(f"  Mice:          {len(ranking_df):>4} rows  →  {mouse_path}")
    print(f"  Samples:       {len(sample_df):>4} rows  →  {sample_path}")
    print(f"  Assay params:  {len(assay_long_df):>4} rows  →  {assay_long_path}")
    if not assay_long_df.empty:
        print(f"  Assay wide:         →  {assay_wide_path}")
    print(f"  Manifest:           →  {out_dir / MANIFEST_FILE}")

    print()
    print("Data freshness:")
    for osd_id, pulled_at, changed in study_status:
        status = "UPDATED" if changed else "unchanged"
        print(f"  {osd_id:12s}  {status:10s}  pulled_at={pulled_at}")

    print()
    print("Fill-rate / coverage (mouse_metadata):")
    for col in ["osd_ids", "n_organs", "n_assay_types",
                "informativeness_score", "informativeness_rank"]:
        if col in ranking_df.columns:
            n = (ranking_df[col].notna() & (ranking_df[col].astype(str) != "")).sum()
            print(f"  {col:25s}: {n}/{len(ranking_df)}")

    print()
    print("Fill-rate (sample_metadata):")
    for col in ["mouse_strain", "mouse_sex", "age", "space_or_ground",
                "assay_category", "assay_name", "material_type"]:
        if col in sample_df.columns:
            n = (sample_df[col].notna() & (sample_df[col] != "")).sum()
            print(f"  {col:25s}: {n}/{len(sample_df)}")

    if not assay_long_df.empty:
        print()
        print("Assay parameter coverage:")
        for cat, grp in assay_long_df.groupby("assay_category"):
            n_params = grp["parameter_name"].nunique()
            n_filled = (grp["parameter_value"].notna() & (grp["parameter_value"] != "")).sum()
            print(f"  {cat}: {n_params} distinct params, {n_filled} filled values")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="NASA OSDR Full Export — mouse + sample + assay parameter tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--osd", nargs="+", metavar="OSD_ID",
        help="One or more OSD study IDs (e.g. OSD-48 OSD-87 OSD-168)",
    )
    source.add_argument(
        "--mission", metavar="MISSION",
        help="Mission name — discovers constituent OSDs via live API (e.g. RR-1, RR-3)",
    )
    source.add_argument(
        "--input", type=Path, metavar="CSV",
        help="CSV file with OSD_study column (and optionally RR_mission column)",
    )
    p.add_argument(
        "-o", "--output", type=Path, default=Path("outputs/full_export"),
        help="Output directory (default: outputs/full_export)",
    )
    p.add_argument("--osd-col",     default="OSD_study",  help="OSD ID column name in --input CSV")
    p.add_argument("--mission-col", default="RR_mission", help="Mission column name in --input CSV")
    p.add_argument("--cache-dir",   type=Path)
    p.add_argument("--isa-tab-dir", type=Path)

    assay_group = p.add_mutually_exclusive_group()
    assay_group.add_argument(
        "--include-assay", nargs="+", metavar="TYPE", dest="include_assays",
        help=(
            "Only parse these assay types. Case-insensitive substring match against "
            "the assay filename. "
            "E.g. --include-assay rna-seq mass-spec"
        ),
    )
    assay_group.add_argument(
        "--exclude-assay", nargs="+", metavar="TYPE", dest="exclude_assays",
        help=(
            "Skip these assay types. Useful for reducing memory on large missions. "
            "E.g. --exclude-assay western-blot calcium-uptake  "
            "Known types: rna-seq, dna-methylation, rna-methylation, mass-spec, "
            "metabolomics, atac-seq, behavior, atpase, calcium-uptake, "
            "echocardiogram, microscopy, western-blot, bone-microstructure, microarray"
        ),
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    defaults = get_default_paths()
    cache_dir   = args.cache_dir   or defaults["cache_dir"]
    isa_tab_dir = args.isa_tab_dir or defaults["isa_tab_dir"]

    client   = OSDRClient(cache_dir=cache_dir, isa_tab_dir=isa_tab_dir)
    resolver = MissionResolver(client=client, cache_dir=cache_dir)

    osd_pairs: list[tuple[str, str]] = []

    if args.mission:
        print(f"Resolving mission '{args.mission}' via live API …")
        osd_pairs = _discover_mission_osds(args.mission, client, resolver)
        if not osd_pairs:
            print(
                f"Error: no studies found for mission '{args.mission}'.\n"
                f"Check the mission name (e.g. RR-1, RR-3, RR-10) and that the API is reachable.",
                file=sys.stderr,
            )
            return 1
        print(f"Resolved {len(osd_pairs)} studies: {', '.join(o for o, _ in osd_pairs)}\n")

    elif args.osd:
        print("Verifying OSD IDs via live API …")
        valid_ids = _resolve_input_osds(args.osd, client)
        if not valid_ids:
            print("Error: none of the provided OSD IDs could be verified.", file=sys.stderr)
            return 1
        print()
        osd_pairs = [(osd_id, "") for osd_id in valid_ids]

    else:  # --input
        raw_pairs = _read_osd_list_from_csv(args.input, args.osd_col, args.mission_col)
        print(f"Loaded {len(raw_pairs)} OSD IDs from {args.input}")
        print("Verifying OSD IDs via live API …")
        verified = []
        for raw_osd, mission in raw_pairs:
            osd_id = OSDRClient.normalize_osd_id(raw_osd)
            print(f"  Verifying {osd_id} ...", end=" ", flush=True)
            if _verify_osd_exists(osd_id, client):
                print("OK")
                verified.append((osd_id, mission))
            else:
                print("NOT FOUND — skipping")
        if not verified:
            print("Error: no valid OSD IDs found in input CSV.", file=sys.stderr)
            return 1
        print()
        osd_pairs = verified

    run_export(
        osd_pairs=osd_pairs,
        out_dir=args.output,
        cache_dir=cache_dir,
        isa_tab_dir=isa_tab_dir,
        include_assays=getattr(args, "include_assays", None),
        exclude_assays=getattr(args, "exclude_assays", None),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
