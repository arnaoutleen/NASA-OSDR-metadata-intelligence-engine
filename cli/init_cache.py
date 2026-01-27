#!/usr/bin/env python3
"""
NASA OSDR Metadata Intelligence Engine - Cache Initialization

Downloads and caches ISA-Tab data for demo studies so the pipeline
works immediately out of the box.

Usage:
    python -m cli.init_cache
    python -m cli.init_cache --studies OSD-102 OSD-242
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.osdr_client import OSDRClient


# Default studies used in demo test files
DEMO_STUDIES = [
    "OSD-102",   # Rodent Research 1 - kidney
    "OSD-202",   # Ground study - brain  
    "OSD-242",   # Spaceflight - liver
    "OSD-479",   # Rodent Research 9 - liver
    "OSD-477",   # Rat study - femur
    "OSD-546",   # Human cells - bone marrow
    "OSD-661",   # Hindlimb unloading - spine
    "OSD-207",   # Drosophila - head
]


def init_cache(studies: list[str], verbose: bool = True) -> dict:
    """
    Download and cache ISA-Tab data for specified studies.
    
    Args:
        studies: List of OSD IDs to cache
        verbose: Print progress messages
        
    Returns:
        Dict with success/failure counts
    """
    cache_dir = PROJECT_ROOT / "resources" / "osdr_api" / "raw"
    isa_dir = PROJECT_ROOT / "resources" / "isa_tab"
    
    # Ensure directories exist
    cache_dir.mkdir(parents=True, exist_ok=True)
    isa_dir.mkdir(parents=True, exist_ok=True)
    
    client = OSDRClient(cache_dir=cache_dir, isa_tab_dir=isa_dir)
    
    results = {"success": 0, "failed": 0, "studies": {}}
    
    if verbose:
        print("=" * 60)
        print("NASA OSDR Metadata Intelligence Engine")
        print("Cache Initialization")
        print("=" * 60)
        print(f"\nDownloading ISA-Tab data for {len(studies)} studies...")
        print(f"Cache directory: {cache_dir}")
        print(f"ISA-Tab directory: {isa_dir}\n")
    
    for i, osd_id in enumerate(studies, 1):
        # Normalize OSD ID
        osd_id = client.normalize_osd_id(osd_id)
        
        if verbose:
            print(f"[{i}/{len(studies)}] {osd_id}...", end=" ", flush=True)
        
        try:
            # Fetch study metadata first
            metadata = client.fetch_study_json(osd_id, use_cache=False)
            
            # Download ISA-Tab
            isa_path = client.download_isa_tab(osd_id)
            
            if isa_path and isa_path.exists():
                # Count files
                files = list(isa_path.glob("*.txt"))
                results["success"] += 1
                results["studies"][osd_id] = {"status": "success", "files": len(files)}
                
                if verbose:
                    print(f"✓ ({len(files)} files)")
            else:
                results["failed"] += 1
                results["studies"][osd_id] = {"status": "failed", "error": "ISA-Tab not available"}
                
                if verbose:
                    print("✗ (ISA-Tab not available)")
                    
        except Exception as e:
            results["failed"] += 1
            results["studies"][osd_id] = {"status": "failed", "error": str(e)}
            
            if verbose:
                print(f"✗ (Error: {e})")
    
    if verbose:
        print("\n" + "=" * 60)
        print(f"COMPLETE: {results['success']}/{len(studies)} studies cached")
        print("=" * 60)
        
        if results["failed"] > 0:
            print(f"\n⚠ {results['failed']} studies failed:")
            for osd_id, info in results["studies"].items():
                if info["status"] == "failed":
                    print(f"  - {osd_id}: {info['error']}")
        
        print("\nYou can now run the pipeline:")
        print("  python -m cli.process_csv resources/test_inputs/demo/realworld_rodent_research.csv --validate")
    
    return results


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Initialize OSDR data cache for the metadata engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Initialize cache with all demo studies
    python -m cli.init_cache
    
    # Initialize specific studies only
    python -m cli.init_cache --studies OSD-102 OSD-242 OSD-479
    
    # Quiet mode
    python -m cli.init_cache -q
        """,
    )
    
    parser.add_argument(
        "--studies",
        nargs="+",
        default=DEMO_STUDIES,
        help=f"OSD IDs to cache (default: {', '.join(DEMO_STUDIES)})",
    )
    
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress output",
    )
    
    args = parser.parse_args()
    
    results = init_cache(args.studies, verbose=not args.quiet)
    
    # Exit with error if any failed
    return 0 if results["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())



