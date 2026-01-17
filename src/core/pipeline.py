"""
NASA OSDR Metadata Intelligence Engine - Pipeline Orchestration

This module provides the main pipeline orchestration for CSV enrichment,
coordinating API fetching, parsing, enrichment, and output generation.

The pipeline:
1. Loads input CSV
2. Groups rows by OSD ID
3. Fetches metadata for each study (with caching)
4. Enriches each row with provenance tracking
5. Outputs enriched CSV and provenance logs
"""

import csv
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from src.core.osdr_client import OSDRClient
from src.core.isa_parser import ISAParser
from src.core.enrichment_rules import enrich_row, EnrichmentResult
from src.core.provenance import ProvenanceTracker
from src.validation.conflict_checker import ConflictChecker, ConflictReport
from src.utils.flexible_loader import load_flexible, FlexibleLoaderResult


@dataclass
class PipelineConfig:
    """Configuration for the enrichment pipeline."""
    
    # Input/Output paths
    input_csv_path: Path
    output_csv_path: Path
    provenance_log_path: Path
    validation_report_path: Optional[Path] = None
    
    # Cache directories
    cache_dir: Path = field(default_factory=lambda: Path("resources/osdr_api/raw"))
    isa_tab_dir: Path = field(default_factory=lambda: Path("resources/isa_tab"))
    
    # Processing options
    use_cache: bool = True
    clear_cache: bool = False
    fetch_isa_tab: bool = True
    
    # Column mapping (input column names) - these are canonical names after alias mapping
    osd_id_column: str = "osd_id"
    sample_id_column: str = "sample_id"
    mission_column: str = "RR_mission"
    
    # Flexible loading options
    use_flexible_loader: bool = True
    
    # Output directory for validation reports
    validation_dir: Path = field(default_factory=lambda: Path("outputs/validation_reports"))


@dataclass
class PipelineResult:
    """Result of a pipeline run."""
    
    # Counts
    total_rows: int = 0
    enriched_rows: int = 0
    studies_processed: int = 0
    studies_fetched: int = 0
    studies_from_cache: int = 0
    conflict_count: int = 0
    
    # Errors and warnings
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Output paths
    output_csv_path: Optional[Path] = None
    provenance_log_path: Optional[Path] = None
    validation_report_path: Optional[Path] = None
    conflict_report_path: Optional[Path] = None
    
    # Timing
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration_seconds(self) -> float:
        """Get pipeline duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0


class Pipeline:
    """
    Main enrichment pipeline orchestrator.
    
    Coordinates:
    - OSDR API client for metadata fetching
    - ISA-Tab parser for sample-level data
    - Enrichment rules for filling missing fields
    - Provenance tracking for auditability
    """
    
    def __init__(self, config: PipelineConfig):
        """
        Initialize pipeline with configuration.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config
        
        # Initialize client and parser
        self.client = OSDRClient(
            cache_dir=config.cache_dir,
            isa_tab_dir=config.isa_tab_dir,
        )
        self.parser = ISAParser(isa_tab_dir=config.isa_tab_dir)
        
        # Initialize provenance tracker
        self.tracker = ProvenanceTracker()
        
        # Initialize conflict checker
        self.conflict_checker = ConflictChecker()
        
        # Metadata cache for current run
        self._metadata_cache: Dict[str, Dict[str, Any]] = {}
    
    def run(self) -> PipelineResult:
        """
        Execute the enrichment pipeline.
        
        Returns:
            PipelineResult with statistics and output paths
        """
        result = PipelineResult(start_time=datetime.now())
        
        print("=" * 70)
        print("NASA OSDR Metadata Intelligence Engine")
        print("Science-Grade Metadata Enrichment Pipeline")
        print("=" * 70)
        
        # Optionally clear cache
        if self.config.clear_cache:
            print("\n[0] Clearing API cache...")
            self.client.clear_cache()
        
        # Step 1: Load input CSV
        print("\n[1/5] Loading input CSV...")
        rows, headers = self._load_csv()
        
        if not rows:
            result.errors.append(f"Failed to load CSV: {self.config.input_csv_path}")
            result.end_time = datetime.now()
            return result
        
        result.total_rows = len(rows)
        print(f"  Loaded {len(rows)} rows with {len(headers)} columns")
        
        # Step 2: Group by OSD ID
        print("\n[2/5] Grouping rows by OSD ID...")
        osd_groups = self._group_by_osd_id(rows)
        result.studies_processed = len(osd_groups)
        print(f"  Found {len(osd_groups)} unique studies")
        
        # Step 3: Fetch metadata for each study
        print("\n[3/5] Fetching metadata from OSDR...")
        self._fetch_all_metadata(osd_groups.keys(), result)
        
        # Step 4: Enrich rows
        print("\n[4/6] Enriching rows...")
        enriched_rows = self._enrich_rows(rows, result)
        
        # Step 5: Run conflict checking
        print("\n[5/6] Running conflict checks...")
        self._run_conflict_checks(rows, result)
        
        # Step 6: Write outputs
        print("\n[6/6] Writing outputs...")
        self._write_outputs(enriched_rows, headers, result)
        
        result.end_time = datetime.now()
        
        # Print summary
        self._print_summary(result)
        
        return result
    
    def _load_csv(self) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Load input file using flexible loader.
        
        Supports CSV, TSV, Excel, and JSON formats with automatic
        column alias mapping for researcher-defined schemas.
        """
        try:
            if not self.config.input_csv_path.exists():
                print(f"  ERROR: File not found: {self.config.input_csv_path}")
                return [], []
            
            if self.config.use_flexible_loader:
                # Use flexible loader with alias mapping
                result: FlexibleLoaderResult = load_flexible(self.config.input_csv_path)
                
                # Report any warnings
                if result.warnings:
                    print(f"  Warnings during loading:")
                    for warning in result.warnings[:5]:  # Show first 5
                        print(f"    - {warning}")
                    if len(result.warnings) > 5:
                        print(f"    ... and {len(result.warnings) - 5} more")
                
                # Report column mapping if aliases were used
                alias_mappings = [
                    f"{orig} → {canon}"
                    for orig, canon in result.header_mapping.items()
                    if orig != canon
                ]
                if alias_mappings:
                    print(f"  Column aliases applied: {', '.join(alias_mappings[:3])}")
                    if len(alias_mappings) > 3:
                        print(f"    ... and {len(alias_mappings) - 3} more")
                
                if result.skipped_rows > 0:
                    print(f"  Skipped {result.skipped_rows} rows (missing required fields)")
                
                return result.rows, result.normalized_headers
            else:
                # Fallback to direct CSV loading
                with open(self.config.input_csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    headers = reader.fieldnames or []
                    rows = list(reader)
                
                return rows, list(headers)
            
        except Exception as e:
            print(f"  ERROR: {e}")
            return [], []
    
    def _group_by_osd_id(
        self,
        rows: List[Dict[str, Any]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Group rows by OSD ID."""
        groups: Dict[str, List[Dict[str, Any]]] = {}
        
        for row in rows:
            osd_id = row.get(self.config.osd_id_column, "")
            if not osd_id:
                # Try alternative column names
                for col in ["OSD", "osd_id", "study_id"]:
                    if col in row and row[col]:
                        osd_id = row[col]
                        break
            
            if osd_id:
                osd_id = OSDRClient.normalize_osd_id(osd_id)
                if osd_id not in groups:
                    groups[osd_id] = []
                groups[osd_id].append(row)
        
        return groups
    
    def _fetch_all_metadata(
        self,
        osd_ids: Set[str],
        result: PipelineResult,
    ) -> None:
        """Fetch metadata for all OSD IDs."""
        total = len(osd_ids)
        
        for i, osd_id in enumerate(sorted(osd_ids), 1):
            print(f"  [{i}/{total}] Fetching {osd_id}...", end=" ")
            
            try:
                metadata = self.client.fetch_study_json(
                    osd_id,
                    use_cache=self.config.use_cache,
                )
                
                if metadata:
                    self._metadata_cache[osd_id] = metadata
                    
                    # Optionally fetch and merge ISA-Tab
                    if self.config.fetch_isa_tab:
                        isa_dir = self.client.download_isa_tab(osd_id)
                        if isa_dir:
                            self._metadata_cache[osd_id] = self.parser.merge_with_api_metadata(
                                metadata, osd_id
                            )
                    
                    # Report sample count AFTER ISA-Tab merge
                    samples_count = len(self._metadata_cache[osd_id].get("samples", []))
                    result.studies_fetched += 1
                    print(f"✓ ({samples_count} samples)")
                else:
                    print("✗ (not available)")
                    result.warnings.append(f"No metadata available for {osd_id}")
                    
            except Exception as e:
                print(f"✗ (error: {e})")
                result.errors.append(f"Failed to fetch {osd_id}: {e}")
    
    def _enrich_rows(
        self,
        rows: List[Dict[str, Any]],
        result: PipelineResult,
    ) -> List[Dict[str, Any]]:
        """Enrich all rows."""
        enriched_rows = []
        enriched_count = 0
        
        for row in rows:
            # Get OSD ID
            osd_id = row.get(self.config.osd_id_column, "")
            if osd_id:
                osd_id = OSDRClient.normalize_osd_id(osd_id)
            
            # Get metadata
            metadata = self._metadata_cache.get(osd_id, {})
            
            # Enrich row
            if metadata:
                enrichment_result = enrich_row(
                    row=row,
                    osdr_study_json=metadata,
                    isa_metadata=None,  # Already merged
                    tracker=self.tracker,
                )
                
                enriched_rows.append(enrichment_result.enriched_row)
                
                if enrichment_result.provenance_entries:
                    enriched_count += 1
            else:
                enriched_rows.append(row)
        
        result.enriched_rows = enriched_count
        print(f"  Enriched {enriched_count}/{len(rows)} rows")
        
        return enriched_rows
    
    def _run_conflict_checks(
        self,
        rows: List[Dict[str, Any]],
        result: PipelineResult,
    ) -> None:
        """
        Run conflict checks between API and ISA-Tab data sources.
        
        Compares metadata values from different sources and records
        any discrepancies for review.
        """
        conflict_count = 0
        
        # Cache ISA-Tab parsing results to avoid re-parsing for every row
        isa_cache: Dict[str, Any] = {}
        
        for row in rows:
            # Get OSD ID and sample ID
            osd_id = row.get(self.config.osd_id_column, "")
            if osd_id:
                osd_id = OSDRClient.normalize_osd_id(osd_id)
            
            sample_id = row.get(self.config.sample_id_column, "")
            if not sample_id:
                for col in ["sample_id", "Sample Name", "sample_name"]:
                    if col in row and row[col]:
                        sample_id = row[col]
                        break
            
            if not osd_id or not sample_id:
                continue
            
            # Get metadata for this study
            metadata = self._metadata_cache.get(osd_id, {})
            if not metadata:
                continue
            
            # Find matching sample in API data
            samples = metadata.get("samples", [])
            api_sample = None
            for s in samples:
                s_id = s.get("id", s.get("name", s.get("sample_name", "")))
                if s_id == sample_id or sample_id in s_id or s_id in sample_id:
                    api_sample = s
                    break
            
            if not api_sample:
                continue
            
            # Get ISA-Tab data if available (cached per study)
            isa_sample = {}
            if osd_id not in isa_cache:
                isa_cache[osd_id] = self.parser.parse(osd_id)
            
            isa_metadata = isa_cache[osd_id]
            if isa_metadata:
                for isa_s in isa_metadata.samples:
                    if isa_s.sample_name == sample_id or sample_id in isa_s.sample_name:
                        isa_sample = isa_s.to_dict()
                        break
            
            # Build API data dict for comparison
            api_data = {
                "strain": api_sample.get("strain", ""),
                "sex": api_sample.get("sex", ""),
                "age": api_sample.get("age", ""),
                "material_type": api_sample.get("material_type", ""),
            }
            
            # Build ISA data dict for comparison
            isa_data = {
                "strain": isa_sample.get("strain", ""),
                "sex": isa_sample.get("sex", ""),
                "age": isa_sample.get("age", ""),
                "material_type": isa_sample.get("material_type", ""),
            }
            
            # Check for conflicts
            conflicts = self.conflict_checker.check_sample(
                osd_id=osd_id,
                sample_id=sample_id,
                api_data=api_data,
                isa_data=isa_data,
            )
            
            conflict_count += len(conflicts)
        
        result.conflict_count = conflict_count
        report = self.conflict_checker.get_report()
        
        if conflict_count > 0:
            print(f"  Found {conflict_count} conflicts across {len(report.conflicts_by_study)} studies")
        else:
            print(f"  No conflicts detected")
    
    def _write_outputs(
        self,
        enriched_rows: List[Dict[str, Any]],
        headers: List[str],
        result: PipelineResult,
    ) -> None:
        """Write output files."""
        # Ensure output directories exist
        self.config.output_csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.provenance_log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Extend headers with any new fields from enrichment
        # New fields to add if they were enriched but not in original headers
        potential_new_fields = [
            "age", "mouse_id", "n_mice_total", "Has_RNAseq", 
            "mouse_genetic_variant", "mouse_source", "time_in_space",
            "study purpose", "n_RNAseq_mice", "age_when_sent_to_space"
        ]
        extended_headers = list(headers)
        
        for new_field in potential_new_fields:
            if new_field not in extended_headers:
                # Check if any row has this field populated
                has_data = any(row.get(new_field) for row in enriched_rows)
                if has_data:
                    extended_headers.append(new_field)
        
        # Write enriched CSV
        try:
            with open(self.config.output_csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=extended_headers, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(enriched_rows)
            
            result.output_csv_path = self.config.output_csv_path
            print(f"  ✓ Wrote enriched CSV: {self.config.output_csv_path}")
            
        except Exception as e:
            result.errors.append(f"Failed to write CSV: {e}")
            print(f"  ✗ Failed to write CSV: {e}")
        
        # Write provenance log
        try:
            self.tracker.export_json(self.config.provenance_log_path)
            result.provenance_log_path = self.config.provenance_log_path
            print(f"  ✓ Wrote provenance log: {self.config.provenance_log_path}")
            
        except Exception as e:
            result.errors.append(f"Failed to write provenance log: {e}")
            print(f"  ✗ Failed to write provenance log: {e}")
        
        # Optionally write validation report
        if self.config.validation_report_path:
            try:
                self.config.validation_report_path.parent.mkdir(parents=True, exist_ok=True)
                self._write_validation_report(result)
                result.validation_report_path = self.config.validation_report_path
                print(f"  ✓ Wrote validation report: {self.config.validation_report_path}")
                
            except Exception as e:
                result.warnings.append(f"Failed to write validation report: {e}")
        
        # Write conflict report if conflicts were found
        if result.conflict_count > 0:
            try:
                # Derive conflict report path from input file name
                input_stem = self.config.input_csv_path.stem
                conflict_report_path = (
                    self.config.output_csv_path.parent.parent / 
                    "validation_reports" / 
                    f"{input_stem}_conflicts.txt"
                )
                conflict_report_path.parent.mkdir(parents=True, exist_ok=True)
                self._write_conflict_report(conflict_report_path)
                result.conflict_report_path = conflict_report_path
                print(f"  ✓ Wrote conflict report: {conflict_report_path}")
                
            except Exception as e:
                result.warnings.append(f"Failed to write conflict report: {e}")
    
    def _write_validation_report(self, result: PipelineResult) -> None:
        """Write human-readable validation report."""
        with open(self.config.validation_report_path, "w", encoding="utf-8") as f:
            f.write("NASA OSDR METADATA ENRICHMENT - VALIDATION REPORT\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 60 + "\n\n")
            
            # Summary
            f.write("PIPELINE SUMMARY\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total rows processed: {result.total_rows}\n")
            f.write(f"Rows enriched: {result.enriched_rows}\n")
            f.write(f"Studies processed: {result.studies_processed}\n")
            f.write(f"Duration: {result.duration_seconds:.2f} seconds\n\n")
            
            # Provenance summary
            f.write("PROVENANCE SUMMARY\n")
            f.write("-" * 40 + "\n")
            summary = self.tracker.get_summary()
            for field_name, sources in sorted(summary.items()):
                total = sum(sources.values())
                f.write(f"\n{field_name}: {total} enrichments\n")
                for source, count in sorted(sources.items()):
                    f.write(f"  - {source}: {count}\n")
            
            # Confidence breakdown
            f.write("\nCONFIDENCE BREAKDOWN\n")
            f.write("-" * 40 + "\n")
            conf_stats = self.tracker.get_confidence_stats()
            for level, count in sorted(conf_stats.items()):
                f.write(f"{level}: {count}\n")
            
            # Conflicts
            if self.tracker.conflicts:
                f.write(f"\nCONFLICTS DETECTED: {len(self.tracker.conflicts)}\n")
                f.write("-" * 40 + "\n")
                for conflict in self.tracker.conflicts[:20]:
                    f.write(f"{conflict.osd_id}/{conflict.sample_id}: {conflict.field_name}\n")
                    f.write(f"  Values: {conflict.conflicting_values}\n")
            
            # Errors and warnings
            if result.errors:
                f.write(f"\nERRORS: {len(result.errors)}\n")
                f.write("-" * 40 + "\n")
                for error in result.errors:
                    f.write(f"  - {error}\n")
            
            if result.warnings:
                f.write(f"\nWARNINGS: {len(result.warnings)}\n")
                f.write("-" * 40 + "\n")
                for warning in result.warnings[:50]:
                    f.write(f"  - {warning}\n")
    
    def _write_conflict_report(self, report_path: Path) -> None:
        """Write conflict report to file."""
        report = self.conflict_checker.get_report()
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("NASA OSDR METADATA ENRICHMENT - CONFLICT REPORT\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 70 + "\n\n")
            
            f.write(f"Total Conflicts: {report.total_conflicts}\n")
            f.write(f"Studies Affected: {len(report.conflicts_by_study)}\n\n")
            
            # Summary by field
            if report.conflicts_by_field:
                f.write("CONFLICTS BY FIELD\n")
                f.write("-" * 40 + "\n")
                for field_name, count in sorted(report.conflicts_by_field.items()):
                    f.write(f"  {field_name}: {count}\n")
                f.write("\n")
            
            # Summary by study
            if report.conflicts_by_study:
                f.write("CONFLICTS BY STUDY\n")
                f.write("-" * 40 + "\n")
                for osd_id, count in sorted(report.conflicts_by_study.items()):
                    f.write(f"  {osd_id}: {count}\n")
                f.write("\n")
            
            # Detailed conflict listing
            f.write("DETAILED CONFLICTS\n")
            f.write("-" * 40 + "\n")
            for conflict in report.conflict_entries:
                f.write(f"\n[{conflict.field_name}] {conflict.osd_id}/{conflict.sample_id}\n")
                for source, value in conflict.conflicting_values.items():
                    f.write(f"  {source}: {value}\n")
                if conflict.notes:
                    f.write(f"  Note: {conflict.notes}\n")
    
    def _print_summary(self, result: PipelineResult) -> None:
        """Print pipeline summary."""
        print("\n" + "=" * 70)
        print("PIPELINE COMPLETE")
        print("=" * 70)
        
        print(f"\nRows: {result.enriched_rows}/{result.total_rows} enriched")
        print(f"Studies: {result.studies_fetched}/{result.studies_processed} fetched")
        print(f"Conflicts: {result.conflict_count} detected")
        print(f"Duration: {result.duration_seconds:.2f} seconds")
        
        if result.output_csv_path:
            print(f"\nOutputs:")
            print(f"  CSV: {result.output_csv_path}")
            print(f"  Provenance: {result.provenance_log_path}")
            if result.validation_report_path:
                print(f"  Validation: {result.validation_report_path}")
            if result.conflict_report_path:
                print(f"  Conflicts: {result.conflict_report_path}")
        
        if result.errors:
            print(f"\n⚠ {len(result.errors)} errors occurred")
        
        # Print top 5 conflicts if validation was requested
        if self.config.validation_report_path and result.conflict_count > 0:
            report = self.conflict_checker.get_report()
            print(f"\n{'─' * 70}")
            print("TOP CONFLICTS (up to 5):")
            print("─" * 70)
            for conflict in report.conflict_entries[:5]:
                print(f"[{conflict.field_name}] {conflict.osd_id}/{conflict.sample_id}")
                for source, value in conflict.conflicting_values.items():
                    print(f"    {source}: {value}")
            if result.conflict_count > 5:
                print(f"\n... and {result.conflict_count - 5} more (see {result.conflict_report_path})")
        
        # Print provenance summary
        self.tracker.print_report()


def run_pipeline(
    input_csv_path: Path,
    output_csv_path: Path,
    provenance_log_path: Path,
    validation_report_path: Optional[Path] = None,
    cache_dir: Optional[Path] = None,
    isa_tab_dir: Optional[Path] = None,
    use_cache: bool = True,
    clear_cache: bool = False,
) -> PipelineResult:
    """
    Run the enrichment pipeline with specified paths.
    
    This is the main entry point for pipeline execution.
    
    Args:
        input_csv_path: Path to input CSV file
        output_csv_path: Path to write enriched CSV
        provenance_log_path: Path to write provenance JSON log
        validation_report_path: Optional path for validation report
        cache_dir: Optional custom cache directory
        isa_tab_dir: Optional custom ISA-Tab directory
        use_cache: Whether to use cached metadata
        clear_cache: Whether to clear cache before running
        
    Returns:
        PipelineResult with statistics and output paths
    """
    # Build config
    config = PipelineConfig(
        input_csv_path=Path(input_csv_path),
        output_csv_path=Path(output_csv_path),
        provenance_log_path=Path(provenance_log_path),
        validation_report_path=Path(validation_report_path) if validation_report_path else None,
        cache_dir=Path(cache_dir) if cache_dir else Path("resources/osdr_api/raw"),
        isa_tab_dir=Path(isa_tab_dir) if isa_tab_dir else Path("resources/isa_tab"),
        use_cache=use_cache,
        clear_cache=clear_cache,
    )
    
    # Run pipeline
    pipeline = Pipeline(config)
    return pipeline.run()

