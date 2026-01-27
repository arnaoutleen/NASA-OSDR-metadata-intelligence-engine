#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - Sample Expansion CLI

Expand OSD IDs into sample-level rows by parsing ISA-Tab files.

This tool takes a CSV with OSD IDs as input and generates a detailed
sample-level output with all metadata fields populated from ISA-Tab
Study and Assay files.

Usage:
    python -m cli.expand_samples input.csv -o output.csv
    python -m cli.expand_samples input.csv --no-grouping
"""

import argparse
import csv
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.sample_expander import SampleExpander, SAMPLE_OUTPUT_COLUMNS
from src.utils.config import get_default_paths


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="NASA OSDR Sample Expansion - Generate sample-level rows from OSD IDs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Basic usage - expand OSD IDs to samples
    python -m cli.expand_samples resources/input_study_sumary/input.csv
    
    # Specify output file
    python -m cli.expand_samples input.csv -o outputs/expanded_samples.csv
    
    # Show all values on every row (no grouping)
    python -m cli.expand_samples input.csv --no-grouping
    
    # Specify column names in input file
    python -m cli.expand_samples input.csv --osd-column OSD_study --mission-column RR_mission
        """,
    )
    
    # Required arguments
    parser.add_argument(
        "input_csv",
        type=Path,
        help="Path to input CSV file containing OSD IDs",
    )
    
    # Output options
    parser.add_argument(
        "-o", "--output",
        type=Path,
        dest="output_csv",
        help="Path for output CSV (default: outputs/enriched_csv/<input>_samples.csv)",
    )
    
    # Column mapping
    parser.add_argument(
        "--osd-column",
        type=str,
        default="OSD_study",
        help="Name of column containing OSD IDs (default: OSD_study)",
    )
    
    parser.add_argument(
        "--mission-column",
        type=str,
        default="RR_mission",
        help="Name of column containing mission names (default: RR_mission)",
    )
    
    # Display options
    parser.add_argument(
        "--no-grouping",
        action="store_true",
        help="Show RR_mission and OSD_study on every row (not just first row of each group)",
    )
    
    # Verbosity
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress output except errors",
    )
    
    return parser.parse_args()


def read_input_csv(
    input_path: Path,
    osd_column: str,
    mission_column: str,
) -> list[tuple[str, str]]:
    """
    Read input CSV and extract OSD IDs with optional mission names.
    
    Args:
        input_path: Path to input CSV
        osd_column: Column name containing OSD IDs
        mission_column: Column name containing mission names
        
    Returns:
        List of (osd_id, rr_mission) tuples
    """
    osd_ids = []
    
    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            osd_id = row.get(osd_column, "").strip()
            mission = row.get(mission_column, "").strip()
            
            if osd_id:
                osd_ids.append((osd_id, mission))
    
    return osd_ids


def main() -> int:
    """Main entry point for CLI."""
    args = parse_args()
    
    # Check input file exists
    if not args.input_csv.exists():
        print(f"Error: Input file not found: {args.input_csv}", file=sys.stderr)
        return 1
    
    # Build output path
    if args.output_csv:
        output_csv = args.output_csv
    else:
        defaults = get_default_paths()
        input_stem = args.input_csv.stem
        # Clean up the filename if it has spaces
        clean_stem = input_stem.replace(" ", "_").replace(".", "_")
        output_csv = defaults["enriched_csv_dir"] / f"{clean_stem}_samples.csv"
    
    # Ensure output directory exists
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    
    # Print configuration if verbose
    if args.verbose and not args.quiet:
        print("Configuration:")
        print(f"  Input: {args.input_csv}")
        print(f"  Output: {output_csv}")
        print(f"  OSD column: {args.osd_column}")
        print(f"  Mission column: {args.mission_column}")
        print(f"  Grouping: {not args.no_grouping}")
        print()
    
    # Read input CSV
    if not args.quiet:
        print(f"Reading input file: {args.input_csv}")
    
    osd_ids = read_input_csv(args.input_csv, args.osd_column, args.mission_column)
    
    if not osd_ids:
        print(f"Error: No OSD IDs found in column '{args.osd_column}'", file=sys.stderr)
        return 1
    
    if not args.quiet:
        print(f"Found {len(osd_ids)} OSD IDs to process")
        print()
    
    # Initialize expander
    defaults = get_default_paths()
    expander = SampleExpander(
        cache_dir=defaults["cache_dir"],
        isa_tab_dir=defaults["isa_tab_dir"],
    )
    
    # Process all OSD IDs
    if not args.quiet:
        print("Expanding samples...")
    
    all_rows = []
    total_samples = 0
    failed_osds = []
    
    for i, (osd_id, rr_mission) in enumerate(osd_ids, 1):
        if not args.quiet:
            print(f"  [{i}/{len(osd_ids)}] Processing {osd_id}...", end=" ")
        
        try:
            sample_rows = expander.expand_osd_to_samples(osd_id, rr_mission)
            
            if sample_rows:
                # Convert to dicts and apply grouping
                for j, row in enumerate(sample_rows):
                    row_dict = row.to_dict()
                    
                    # Apply grouping - only show mission/study on first row
                    if not args.no_grouping and j > 0:
                        row_dict["RR_mission"] = ""
                        row_dict["OSD_study"] = ""
                    
                    all_rows.append(row_dict)
                
                total_samples += len(sample_rows)
                if not args.quiet:
                    print(f"✓ ({len(sample_rows)} samples)")
            else:
                failed_osds.append(osd_id)
                if not args.quiet:
                    print("✗ (no samples)")
                    
        except Exception as e:
            failed_osds.append(osd_id)
            if not args.quiet:
                print(f"✗ (error: {e})")
            if args.verbose:
                import traceback
                traceback.print_exc()
    
    # Write output CSV
    if not args.quiet:
        print()
        print(f"Writing output to: {output_csv}")
    
    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SAMPLE_OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(all_rows)
    
    # Print summary
    if not args.quiet:
        print()
        print("=" * 60)
        print("SAMPLE EXPANSION COMPLETE")
        print("=" * 60)
        print(f"OSD IDs processed: {len(osd_ids) - len(failed_osds)}/{len(osd_ids)}")
        print(f"Total samples generated: {total_samples}")
        print(f"Output file: {output_csv}")
        
        if failed_osds:
            print(f"\n⚠ Failed to process {len(failed_osds)} OSD IDs:")
            for osd_id in failed_osds[:10]:
                print(f"  - {osd_id}")
            if len(failed_osds) > 10:
                print(f"  ... and {len(failed_osds) - 10} more")
    
    return 0 if not failed_osds else 1


if __name__ == "__main__":
    sys.exit(main())
