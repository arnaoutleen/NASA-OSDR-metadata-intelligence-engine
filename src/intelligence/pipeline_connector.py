"""
NASA OSDR Metadata Intelligence Engine - Pipeline Connector

This module bridges the research planner to the enrichment CLI/pipeline.
It takes a structured research plan and executes the appropriate pipeline
commands to enrich metadata.

The connector:
1. Takes a ResearchPlan or raw inputs (CSV path, OSD ID)
2. Configures and runs the enrichment pipeline
3. Handles errors with appropriate agent signaling
4. Returns structured results with output paths

Usage:
    from src.intelligence.pipeline_connector import PipelineConnector
    
    connector = PipelineConnector()
    result = connector.execute_enrichment(csv_path="input.csv")
"""

import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from src.intelligence.exceptions import EnrichmentError
from src.intelligence.research_planner import ResearchPlan
from src.core.pipeline import Pipeline, PipelineConfig, PipelineResult
from src.utils.config import get_default_paths


@dataclass
class EnrichmentExecutionResult:
    """
    Result of executing the enrichment pipeline.
    
    Contains all relevant output paths and statistics.
    """
    success: bool
    enriched_csv_path: Optional[Path] = None
    provenance_log_path: Optional[Path] = None
    validation_report_path: Optional[Path] = None
    conflict_report_path: Optional[Path] = None
    rows_enriched: int = 0
    total_rows: int = 0
    conflict_count: int = 0
    duration_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "enriched_csv_path": str(self.enriched_csv_path) if self.enriched_csv_path else None,
            "provenance_log_path": str(self.provenance_log_path) if self.provenance_log_path else None,
            "validation_report_path": str(self.validation_report_path) if self.validation_report_path else None,
            "conflict_report_path": str(self.conflict_report_path) if self.conflict_report_path else None,
            "rows_enriched": self.rows_enriched,
            "total_rows": self.total_rows,
            "conflict_count": self.conflict_count,
            "duration_seconds": self.duration_seconds,
            "errors": self.errors,
            "warnings": self.warnings,
        }
    
    def summary(self) -> str:
        """Generate a human-readable summary."""
        if self.success:
            return (
                f"Enrichment completed successfully.\n"
                f"  Rows: {self.rows_enriched}/{self.total_rows} enriched\n"
                f"  Conflicts: {self.conflict_count} detected\n"
                f"  Duration: {self.duration_seconds:.2f} seconds\n"
                f"  Output: {self.enriched_csv_path}"
            )
        else:
            return f"Enrichment failed: {'; '.join(self.errors)}"


class PipelineConnector:
    """
    Connects the AI planner to the enrichment pipeline.
    
    Provides methods to:
    - Execute enrichment from CSV files
    - Execute enrichment for specific OSD studies
    - Route outputs to correct directories
    - Handle errors with agent-friendly signaling
    """
    
    def __init__(self, output_base: Optional[Path] = None):
        """
        Initialize pipeline connector.
        
        Args:
            output_base: Base directory for outputs (defaults to project outputs/)
        """
        self.defaults = get_default_paths()
        self.output_base = output_base or self.defaults["output_dir"]
    
    def execute_enrichment(
        self,
        csv_path: Optional[Union[str, Path]] = None,
        osd_id: Optional[str] = None,
        plan: Optional[ResearchPlan] = None,
        use_cache: bool = True,
        clear_cache: bool = False,
        generate_validation: bool = True,
    ) -> EnrichmentExecutionResult:
        """
        Execute the enrichment pipeline.
        
        Args:
            csv_path: Path to input CSV file
            osd_id: OSD study ID (alternative to csv_path)
            plan: Optional ResearchPlan for context
            use_cache: Whether to use cached metadata
            clear_cache: Whether to clear cache before running
            generate_validation: Whether to generate validation report
            
        Returns:
            EnrichmentExecutionResult with output paths and stats
            
        Raises:
            EnrichmentError: If pipeline execution fails
        """
        if not csv_path and not osd_id:
            raise EnrichmentError(
                "Either csv_path or osd_id must be provided",
                stage="input_validation",
            )
        
        # If only OSD ID provided, we need a different approach
        if osd_id and not csv_path:
            return self._execute_single_study(osd_id, use_cache, clear_cache)
        
        # Execute CSV enrichment
        return self._execute_csv_enrichment(
            csv_path=Path(csv_path),
            use_cache=use_cache,
            clear_cache=clear_cache,
            generate_validation=generate_validation,
        )
    
    def _execute_csv_enrichment(
        self,
        csv_path: Path,
        use_cache: bool,
        clear_cache: bool,
        generate_validation: bool,
    ) -> EnrichmentExecutionResult:
        """Execute enrichment for a CSV file."""
        result = EnrichmentExecutionResult(success=False)
        
        # Validate input
        if not csv_path.exists():
            raise EnrichmentError(
                f"Input file not found: {csv_path}",
                stage="input_validation",
            )
        
        # Build output paths
        input_stem = csv_path.stem
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        output_csv = self.defaults["enriched_csv_dir"] / f"{input_stem}_enriched.csv"
        provenance_log = self.defaults["provenance_dir"] / f"{input_stem}_provenance_{timestamp}.json"
        validation_report = None
        if generate_validation:
            validation_report = self.defaults["validation_dir"] / f"{input_stem}_validation_{timestamp}.txt"
        
        # Configure pipeline
        config = PipelineConfig(
            input_csv_path=csv_path,
            output_csv_path=output_csv,
            provenance_log_path=provenance_log,
            validation_report_path=validation_report,
            cache_dir=self.defaults["cache_dir"],
            isa_tab_dir=self.defaults["isa_tab_dir"],
            use_cache=use_cache,
            clear_cache=clear_cache,
            fetch_isa_tab=True,
        )
        
        # Run pipeline
        try:
            pipeline = Pipeline(config)
            pipeline_result = pipeline.run()
            
            # Convert to our result format
            result.success = len(pipeline_result.errors) == 0
            result.enriched_csv_path = pipeline_result.output_csv_path
            result.provenance_log_path = pipeline_result.provenance_log_path
            result.validation_report_path = pipeline_result.validation_report_path
            result.conflict_report_path = pipeline_result.conflict_report_path
            result.rows_enriched = pipeline_result.enriched_rows
            result.total_rows = pipeline_result.total_rows
            result.conflict_count = pipeline_result.conflict_count
            result.duration_seconds = pipeline_result.duration_seconds
            result.errors = pipeline_result.errors
            result.warnings = pipeline_result.warnings
            
        except Exception as e:
            raise EnrichmentError(
                f"Pipeline execution failed: {e}",
                stage="pipeline_execution",
                original_error=e,
            )
        
        return result
    
    def _execute_single_study(
        self,
        osd_id: str,
        use_cache: bool,
        clear_cache: bool,
    ) -> EnrichmentExecutionResult:
        """
        Execute enrichment for a single OSD study.
        
        This uses the process_osd_study CLI command.
        """
        result = EnrichmentExecutionResult(success=False)
        
        # Build command
        cmd = [
            sys.executable, "-m", "cli.process_osd_study",
            osd_id,
            "--samples",
            "--factors",
        ]
        
        if not use_cache:
            cmd.append("--no-cache")
        if clear_cache:
            cmd.append("--clear-cache")
        
        # For single study, we export to JSON
        output_path = self.defaults["output_dir"] / f"{osd_id}_metadata.json"
        cmd.extend(["--export-json", str(output_path)])
        
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
            )
            
            if proc.returncode == 0:
                result.success = True
                result.enriched_csv_path = output_path  # Actually JSON
                result.rows_enriched = 1  # Single study
                result.total_rows = 1
            else:
                result.errors.append(proc.stderr or f"Command failed with code {proc.returncode}")
                
        except subprocess.TimeoutExpired:
            raise EnrichmentError(
                f"Pipeline timed out for {osd_id}",
                osd_id=osd_id,
                stage="pipeline_execution",
            )
        except Exception as e:
            raise EnrichmentError(
                f"Failed to process study {osd_id}: {e}",
                osd_id=osd_id,
                stage="pipeline_execution",
                original_error=e,
            )
        
        return result
    
    def execute_from_plan(
        self,
        plan: ResearchPlan,
        csv_path: Optional[Union[str, Path]] = None,
    ) -> EnrichmentExecutionResult:
        """
        Execute enrichment based on a research plan.
        
        Args:
            plan: The research plan to execute
            csv_path: Path to input CSV (required for now)
            
        Returns:
            EnrichmentExecutionResult
        """
        if not csv_path:
            # Check suggested studies
            if plan.suggested_studies:
                # For now, just process first suggested study
                return self._execute_single_study(
                    osd_id=plan.suggested_studies[0],
                    use_cache=True,
                    clear_cache=False,
                )
            else:
                raise EnrichmentError(
                    "No CSV path provided and no studies suggested",
                    stage="input_validation",
                )
        
        return self.execute_enrichment(
            csv_path=csv_path,
            plan=plan,
            use_cache=True,
            clear_cache=False,
            generate_validation=True,
        )
    
    def get_output_paths(self, input_stem: str) -> Dict[str, Path]:
        """
        Get expected output paths for a given input file.
        
        Args:
            input_stem: The stem (name without extension) of the input file
            
        Returns:
            Dictionary of output paths
        """
        return {
            "enriched_csv": self.defaults["enriched_csv_dir"] / f"{input_stem}_enriched.csv",
            "provenance_log": self.defaults["provenance_dir"] / f"{input_stem}_provenance.json",
            "validation_report": self.defaults["validation_dir"] / f"{input_stem}_validation.txt",
            "conflict_report": self.defaults["validation_dir"] / f"{input_stem}_conflicts.txt",
        }


def execute_enrichment(
    csv_path: Optional[Union[str, Path]] = None,
    osd_id: Optional[str] = None,
    use_cache: bool = True,
    clear_cache: bool = False,
) -> EnrichmentExecutionResult:
    """
    Convenience function to execute enrichment.
    
    Args:
        csv_path: Path to input CSV file
        osd_id: OSD study ID (alternative to csv_path)
        use_cache: Whether to use cached metadata
        clear_cache: Whether to clear cache before running
        
    Returns:
        EnrichmentExecutionResult with output paths and stats
    """
    connector = PipelineConnector()
    return connector.execute_enrichment(
        csv_path=csv_path,
        osd_id=osd_id,
        use_cache=use_cache,
        clear_cache=clear_cache,
    )

