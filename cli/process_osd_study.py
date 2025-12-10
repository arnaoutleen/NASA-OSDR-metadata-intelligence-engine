#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - Single Study Processing CLI

Process a single OSD study and display/export its metadata.

Usage:
    python -m cli.process_osd_study OSD-242
    python -m cli.process_osd_study OSD-242 --export-json
    python -m cli.process_osd_study OSD-242 --download-isa-tab
"""

import argparse
import json
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.osdr_client import OSDRClient
from src.core.isa_parser import ISAParser
from src.utils.config import get_default_paths


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch and display metadata for a single OSDR study",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Display study metadata
    python -m cli.process_osd_study OSD-242
    
    # Export to JSON file
    python -m cli.process_osd_study OSD-242 --export-json study.json
    
    # Download ISA-Tab archive
    python -m cli.process_osd_study OSD-242 --download-isa-tab
    
    # Bypass cache and fetch fresh
    python -m cli.process_osd_study OSD-242 --no-cache
        """,
    )
    
    # Required arguments
    parser.add_argument(
        "osd_id",
        type=str,
        help="OSD study identifier (e.g., OSD-242, 242)",
    )
    
    # Export options
    parser.add_argument(
        "--export-json",
        type=Path,
        metavar="PATH",
        help="Export metadata to JSON file",
    )
    
    parser.add_argument(
        "--download-isa-tab",
        action="store_true",
        help="Download and extract ISA-Tab archive",
    )
    
    # Display options
    parser.add_argument(
        "--samples",
        action="store_true",
        help="Show sample-level details",
    )
    
    parser.add_argument(
        "--factors",
        action="store_true",
        help="Show factor values for each sample",
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of samples to display (default: 10)",
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
        help="Clear cached data for this study before fetching",
    )
    
    return parser.parse_args()


def display_study_metadata(metadata: dict, args: argparse.Namespace) -> None:
    """Display study metadata in a formatted way."""
    print("\n" + "=" * 70)
    print(f"Study: {metadata.get('accession', 'Unknown')}")
    print("=" * 70)
    
    # Study-level info
    print(f"\nTitle: {metadata.get('title', 'N/A')}")
    
    description = metadata.get('description', '')
    if description:
        if len(description) > 200:
            description = description[:200] + "..."
        print(f"\nDescription: {description}")
    
    print(f"\nOrganism: {metadata.get('organism', 'N/A')}")
    print(f"Material Type: {metadata.get('material_type', 'N/A')}")
    print(f"Project Type: {metadata.get('project_type', 'N/A')}")
    print(f"Mission: {metadata.get('mission_name', 'N/A')}")
    
    # Assay info
    assay_types = metadata.get('assay_types', '')
    if assay_types:
        types = [t.strip() for t in assay_types.split("     ") if t.strip()]
        print(f"\nAssay Types: {', '.join(types)}")
    
    # Factor info
    factor_names = metadata.get('factor_names', '')
    if factor_names:
        factors = [f.strip() for f in factor_names.split("     ") if f.strip()]
        print(f"Factors: {', '.join(factors)}")
    
    # Sample summary
    samples = metadata.get('samples', [])
    print(f"\nSamples: {len(samples)}")
    
    # Assay summary
    assays = metadata.get('assays', [])
    print(f"Assays: {len(assays)}")
    
    # Show sample details if requested
    if args.samples and samples:
        print("\n" + "-" * 70)
        print("Sample Details")
        print("-" * 70)
        
        for i, sample in enumerate(samples[:args.limit]):
            sample_id = sample.get('id', sample.get('name', f'Sample_{i}'))
            print(f"\n[{i+1}] {sample_id}")
            print(f"    Strain: {sample.get('strain', 'N/A')}")
            print(f"    Sex: {sample.get('sex', 'N/A')}")
            print(f"    Age: {sample.get('age', 'N/A')}")
            print(f"    Tissue: {sample.get('material_type', 'N/A')}")
            
            if args.factors:
                fv = sample.get('factor_values', {})
                if fv:
                    print("    Factor Values:")
                    for k, v in fv.items():
                        print(f"      - {k}: {v}")
        
        if len(samples) > args.limit:
            print(f"\n... and {len(samples) - args.limit} more samples")
    
    print("\n" + "=" * 70)


def main() -> int:
    """Main entry point for CLI."""
    args = parse_args()
    
    # Get default paths
    defaults = get_default_paths()
    
    # Initialize client
    client = OSDRClient(
        cache_dir=defaults["cache_dir"],
        isa_tab_dir=defaults["isa_tab_dir"],
    )
    
    # Normalize OSD ID
    osd_id = OSDRClient.normalize_osd_id(args.osd_id)
    print(f"Processing study: {osd_id}")
    
    # Clear cache if requested
    if args.clear_cache:
        cache_path = defaults["cache_dir"] / f"{osd_id}.json"
        if cache_path.exists():
            cache_path.unlink()
            print(f"Cleared cache: {cache_path}")
    
    # Fetch metadata
    print("\nFetching metadata...")
    metadata = client.fetch_study_json(
        osd_id,
        use_cache=not args.no_cache,
    )
    
    if not metadata:
        print(f"Error: Could not fetch metadata for {osd_id}", file=sys.stderr)
        return 1
    
    # Download ISA-Tab if requested
    if args.download_isa_tab:
        print("\nDownloading ISA-Tab archive...")
        isa_dir = client.download_isa_tab(osd_id)
        if isa_dir:
            print(f"ISA-Tab extracted to: {isa_dir}")
            
            # Parse and merge
            parser = ISAParser(isa_tab_dir=defaults["isa_tab_dir"])
            metadata = parser.merge_with_api_metadata(metadata, osd_id)
            print(f"Merged ISA-Tab data: {len(metadata.get('samples', []))} samples")
        else:
            print("Warning: Could not download ISA-Tab")
    
    # Display metadata
    display_study_metadata(metadata, args)
    
    # Export to JSON if requested
    if args.export_json:
        args.export_json.parent.mkdir(parents=True, exist_ok=True)
        with open(args.export_json, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        print(f"\nExported to: {args.export_json}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

