#!/usr/bin/env python3
"""
NASA OSDR Assay Parameter Browser

Navigate the assay_parameters_long.csv produced by run_full_export.

Usage
-----
    # List all assay categories present in a file
    python -m cli.query_assays --file outputs/rr1/assay_parameters_long.csv --list-types

    # Show all parameters for a specific assay type
    python -m cli.query_assays --file outputs/rr1/assay_parameters_long.csv --assay rna_sequencing

    # Show a specific parameter across all samples
    python -m cli.query_assays --file outputs/rr1/assay_parameters_long.csv \\
        --assay rna_sequencing --param "Parameter Value[Spike-in Mix Number]"

    # Filter by study
    python -m cli.query_assays --file outputs/rr1/assay_parameters_long.csv \\
        --assay protein_mass_spec --osd OSD-48

    # Export filtered result to CSV
    python -m cli.query_assays --file outputs/rr1/assay_parameters_long.csv \\
        --assay rna_sequencing -o outputs/rr1/rnaseq_params.csv

    # Interactive mode — prompts you to choose filters step by step
    python -m cli.query_assays --file outputs/rr1/assay_parameters_long.csv --interactive
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd


# ---------------------------------------------------------------------------
# Core query helpers
# ---------------------------------------------------------------------------

def load_long(file_path: Path) -> pd.DataFrame:
    """Load assay_parameters_long.csv and normalise key columns."""
    df = pd.read_csv(file_path, dtype=str).fillna("")
    # Normalise column names in case of minor variations
    df.columns = [c.strip() for c in df.columns]
    return df


def list_types(df: pd.DataFrame) -> None:
    """Print every assay category present, with sample/parameter counts."""
    if "assay_category" not in df.columns:
        print("Column 'assay_category' not found.", file=sys.stderr)
        return

    summary = (
        df[df["assay_category"] != ""]
        .groupby("assay_category")
        .agg(
            n_param_rows=("parameter_name", "count"),
            n_distinct_params=("parameter_name", "nunique"),
            n_samples=("sample_id", "nunique"),
            example_params=("parameter_name",
                            lambda x: " | ".join(sorted(x.unique())[:3])),
        )
        .reset_index()
        .sort_values("assay_category")
    )

    print(f"\n{'ASSAY CATEGORY':35s}  {'PARAMS':7s}  {'SAMPLES':7s}  EXAMPLE PARAMETERS")
    print("-" * 100)
    for _, row in summary.iterrows():
        cat   = str(row["assay_category"])[:34]
        n_p   = row["n_distinct_params"]
        n_s   = row["n_samples"]
        ex    = str(row["example_params"])[:50]
        print(f"  {cat:33s}  {n_p:7d}  {n_s:7d}  {ex}")
    print()


def list_params(df: pd.DataFrame, assay_category: str) -> None:
    """Print every parameter name available for an assay category."""
    sub = df[df["assay_category"].str.lower() == assay_category.lower()]
    if sub.empty:
        print(f"No data found for assay category: {assay_category!r}", file=sys.stderr)
        return

    params = (
        sub.groupby("parameter_name")
        .agg(
            n_values=("parameter_value", "count"),
            n_samples=("sample_id", "nunique"),
            unique_values=("parameter_value",
                           lambda x: " | ".join(sorted(x[x != ""].unique())[:5])),
        )
        .reset_index()
        .sort_values("parameter_name")
    )

    print(f"\nParameters for assay category: {assay_category!r}")
    print(f"{'PARAMETER NAME':55s}  {'VALUES':7s}  {'SAMPLES':7s}  EXAMPLE VALUES")
    print("-" * 120)
    for _, row in params.iterrows():
        name = str(row["parameter_name"])[:54]
        n_v  = row["n_values"]
        n_s  = row["n_samples"]
        ex   = str(row["unique_values"])[:50]
        print(f"  {name:53s}  {n_v:7d}  {n_s:7d}  {ex}")
    print()


def query(
    df: pd.DataFrame,
    assay_category: str | None = None,
    param_name: str | None = None,
    osd_id: str | None = None,
    mission: str | None = None,
    payload: str | None = None,
    wide: bool = False,
) -> pd.DataFrame:
    """
    Filter the long table and return the result.

    Parameters
    ----------
    assay_category : filter to one assay type (case-insensitive partial match)
    param_name     : filter to one parameter name (case-insensitive partial match)
    osd_id         : filter to one OSD study
    mission        : filter to a mission (e.g. RR-1)
    payload        : filter to a payload (e.g. SpaceX-4)
    wide           : pivot to wide format (one column per parameter)
    """
    result = df.copy()

    if assay_category:
        mask = result["assay_category"].str.lower().str.contains(
            assay_category.lower(), regex=False
        )
        result = result[mask]

    if param_name:
        mask = result["parameter_name"].str.lower().str.contains(
            param_name.lower(), regex=False
        )
        result = result[mask]

    if osd_id:
        result = result[result["osd_id"].str.upper() == osd_id.upper()]

    if mission and "mission" in result.columns:
        result = result[result["mission"].str.lower() == mission.lower()]

    if payload and "payload" in result.columns:
        result = result[result["payload"].str.lower() == payload.lower()]

    if wide and not result.empty:
        id_cols = [c for c in ["osd_id", "pulled_at", "payload", "mission",
                               "sample_id", "mouse_id", "assay_category",
                               "assay_name"] if c in result.columns]
        try:
            result = result.pivot_table(
                index=id_cols,
                columns="parameter_name",
                values="parameter_value",
                aggfunc="first",
            ).reset_index()
            result.columns.name = None
        except Exception as exc:
            print(f"Warning: could not pivot to wide format — {exc}", file=sys.stderr)

    return result


def display(df: pd.DataFrame, max_rows: int = 40) -> None:
    """Print a compact view of the result."""
    if df.empty:
        print("No results found.")
        return

    print(f"\n{len(df)} rows\n")
    show_cols = [c for c in ["osd_id", "payload", "mission", "sample_id",
                             "assay_category", "parameter_name", "parameter_value"]
                 if c in df.columns]
    to_show = df[show_cols] if show_cols else df

    with pd.option_context("display.max_rows", max_rows,
                           "display.max_colwidth", 60,
                           "display.width", 140):
        print(to_show.head(max_rows).to_string(index=False))

    if len(df) > max_rows:
        print(f"\n... and {len(df) - max_rows} more rows (use -o to export all)")
    print()


# ---------------------------------------------------------------------------
# Interactive mode
# ---------------------------------------------------------------------------

def interactive_mode(df: pd.DataFrame) -> pd.DataFrame:
    """Step-by-step filter prompts."""
    print("\n=== Assay Parameter Browser — Interactive Mode ===\n")

    # Step 1: assay category
    cats = sorted(df["assay_category"].unique())
    print("Available assay categories:")
    for i, c in enumerate(cats, 1):
        print(f"  {i}. {c}")
    choice = input("\nEnter number or partial name (or press Enter to skip): ").strip()
    assay_cat = None
    if choice:
        if choice.isdigit() and 1 <= int(choice) <= len(cats):
            assay_cat = cats[int(choice) - 1]
        else:
            assay_cat = choice
        print(f"  → filtering to: {assay_cat!r}")

    # Step 2: show available params for chosen category
    sub = df[df["assay_category"].str.lower().str.contains(
        assay_cat.lower(), regex=False)] if assay_cat else df
    params = sorted(sub["parameter_name"].unique())

    param_choice = None
    if params:
        print(f"\nAvailable parameters ({len(params)} total):")
        for i, p in enumerate(params, 1):
            print(f"  {i:3d}. {p}")
        choice = input("\nEnter number or partial name (or press Enter to skip): ").strip()
        if choice:
            if choice.isdigit() and 1 <= int(choice) <= len(params):
                param_choice = params[int(choice) - 1]
            else:
                param_choice = choice
            print(f"  → filtering to: {param_choice!r}")

    # Step 3: OSD filter
    osds = sorted(df["osd_id"].unique())
    osd_choice = None
    if len(osds) > 1:
        print(f"\nAvailable studies: {', '.join(osds)}")
        choice = input("Filter to one study? (Enter OSD ID or press Enter to skip): ").strip()
        if choice:
            osd_choice = choice

    # Step 4: wide or long
    fmt = input("\nOutput format — (l)ong [default] or (w)ide? ").strip().lower()
    wide = fmt.startswith("w")

    return query(df, assay_category=assay_cat, param_name=param_choice,
                 osd_id=osd_choice, wide=wide)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="NASA OSDR Assay Parameter Browser",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--file", "-f", type=Path, required=True,
        help="Path to assay_parameters_long.csv from run_full_export",
    )

    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--list-types", action="store_true",
        help="List all assay categories with counts",
    )
    mode.add_argument(
        "--list-params", action="store_true",
        help="List all parameter names for --assay",
    )
    mode.add_argument(
        "--interactive", action="store_true",
        help="Step-by-step interactive filter mode",
    )

    p.add_argument("--assay",   metavar="CATEGORY",
                   help="Filter by assay category (e.g. rna_sequencing, protein_mass_spec)")
    p.add_argument("--param",   metavar="NAME",
                   help="Filter by parameter name (partial match)")
    p.add_argument("--osd",     metavar="OSD_ID",
                   help="Filter to one OSD study (e.g. OSD-48)")
    p.add_argument("--mission", metavar="MISSION",
                   help="Filter by mission (e.g. RR-1)")
    p.add_argument("--payload", metavar="PAYLOAD",
                   help="Filter by payload (e.g. SpaceX-4)")
    p.add_argument("--wide",    action="store_true",
                   help="Pivot output to wide format (one column per parameter)")
    p.add_argument("-o", "--output", type=Path,
                   help="Write filtered result to this CSV file")
    p.add_argument("--max-rows", type=int, default=40,
                   help="Max rows to print to terminal (default: 40)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if not args.file.exists():
        print(f"File not found: {args.file}", file=sys.stderr)
        return 1

    df = load_long(args.file)
    if df.empty:
        print("File is empty.", file=sys.stderr)
        return 1

    # ── Mode dispatch ─────────────────────────────────────────────────────────
    if args.list_types:
        list_types(df)
        return 0

    if args.list_params:
        if not args.assay:
            print("--list-params requires --assay", file=sys.stderr)
            return 1
        list_params(df, args.assay)
        return 0

    if args.interactive:
        result = interactive_mode(df)
    else:
        result = query(
            df,
            assay_category=args.assay,
            param_name=args.param,
            osd_id=args.osd,
            mission=args.mission,
            payload=args.payload,
            wide=args.wide,
        )

    display(result, max_rows=args.max_rows)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(args.output, index=False)
        print(f"Saved {len(result)} rows → {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
