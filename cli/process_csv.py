#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - CSV Processing CLI

Process a CSV file through the metadata enrichment pipeline.

Usage:
    python -m cli.process_csv input.csv -o output.csv
    python -m cli.process_csv input.csv --no-cache
    python -m cli.process_csv input.csv --clear-cache
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.pipeline import run_pipeline, PipelineConfig, Pipeline
from src.utils.config import Config, get_default_paths


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="NASA OSDR Metadata Enrichment Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Basic usage with default output paths
    python -m cli.process_csv resources/study_overview_examples/Yeshasvi_2.csv
    
    # Specify output file
    python -m cli.process_csv input.csv -o outputs/enriched_csv/output.csv
    
    # Force fresh fetch from APIs (bypass cache)
    python -m cli.process_csv input.csv --no-cache
    
    # Clear all cached data first
    python -m cli.process_csv input.csv --clear-cache
    
    # Generate validation report
    python -m cli.process_csv input.csv --validate
        """,
    )
    
    # Required arguments
    parser.add_argument(
        "input_csv",
        type=Path,
        help="Path to input CSV file",
    )
    
    # Output options
    parser.add_argument(
        "-o", "--output",
        type=Path,
        dest="output_csv",
        help="Path for enriched output CSV (default: outputs/enriched_csv/<input>_enriched.csv)",
    )
    
    parser.add_argument(
        "--provenance",
        type=Path,
        dest="provenance_log",
        help="Path for provenance log JSON (default: outputs/provenance_logs/<input>_provenance.json)",
    )
    
    parser.add_argument(
        "--validation-report",
        type=Path,
        dest="validation_report",
        help="Path for validation report (default: outputs/validation_reports/<input>_validation.txt)",
    )
    
    # Cache options
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Don't use cached metadata, fetch fresh from APIs",
    )
    
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear all cached metadata before processing",
    )
    
    # Processing options
    parser.add_argument(
        "--no-isa-tab",
        action="store_true",
        help="Don't download ISA-Tab files (faster, less complete)",
    )
    
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Generate validation report",
    )
    
    # Column mapping (canonical names after flexible loader processes the file)
    parser.add_argument(
        "--osd-column",
        type=str,
        default="osd_id",
        help="Name of OSD ID column (default: osd_id - flexible loader normalizes column names)",
    )
    
    parser.add_argument(
        "--sample-column",
        type=str,
        default="sample_id",
        help="Name of sample ID column (default: sample_id - flexible loader normalizes column names)",
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


def build_output_paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    """Build output paths from arguments or defaults."""
    defaults = get_default_paths()
    input_stem = args.input_csv.stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Output CSV
    if args.output_csv:
        output_csv = args.output_csv
    else:
        output_csv = defaults["enriched_csv_dir"] / f"{input_stem}_enriched.csv"
    
    # Provenance log
    if args.provenance_log:
        provenance_log = args.provenance_log
    else:
        provenance_log = defaults["provenance_dir"] / f"{input_stem}_provenance_{timestamp}.json"
    
    # Validation report
    if args.validation_report:
        validation_report = args.validation_report
    elif args.validate:
        validation_report = defaults["validation_dir"] / f"{input_stem}_validation_{timestamp}.txt"
    else:
        validation_report = None
    
    return output_csv, provenance_log, validation_report


def main() -> int:
    """Main entry point for CLI."""
    args = parse_args()
    
    # Check input file exists
    if not args.input_csv.exists():
        print(f"Error: Input file not found: {args.input_csv}", file=sys.stderr)
        return 1
    
    # Build output paths
    output_csv, provenance_log, validation_report = build_output_paths(args)
    
    # Get default cache directories
    defaults = get_default_paths()
    
    # Print configuration if verbose
    if args.verbose and not args.quiet:
        print("Configuration:")
        print(f"  Input: {args.input_csv}")
        print(f"  Output: {output_csv}")
        print(f"  Provenance: {provenance_log}")
        if validation_report:
            print(f"  Validation: {validation_report}")
        print(f"  Use cache: {not args.no_cache}")
        print(f"  Fetch ISA-Tab: {not args.no_isa_tab}")
        print()
    
    # Build pipeline config
    config = PipelineConfig(
        input_csv_path=args.input_csv,
        output_csv_path=output_csv,
        provenance_log_path=provenance_log,
        validation_report_path=validation_report,
        cache_dir=defaults["cache_dir"],
        isa_tab_dir=defaults["isa_tab_dir"],
        use_cache=not args.no_cache,
        clear_cache=args.clear_cache,
        fetch_isa_tab=not args.no_isa_tab,
        osd_id_column=args.osd_column,
        sample_id_column=args.sample_column,
    )
    
    # Run pipeline
    try:
        pipeline = Pipeline(config)
        result = pipeline.run()
        
        # Print summary unless quiet
        if not args.quiet:
            print(f"\nPipeline completed in {result.duration_seconds:.2f} seconds")
            print(f"Rows enriched: {result.enriched_rows}/{result.total_rows}")
            
            if result.errors:
                print(f"\n⚠ {len(result.errors)} errors occurred:")
                for error in result.errors[:5]:
                    print(f"  - {error}")
                if len(result.errors) > 5:
                    print(f"  ... and {len(result.errors) - 5} more")
        
        return 0 if not result.errors else 1
        
    except Exception as e:
        print(f"Error: Pipeline failed: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

