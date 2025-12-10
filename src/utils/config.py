"""
NASA OSDR Metadata Intelligence Engine - Configuration

This module provides configuration management including:
- Path configuration for inputs, outputs, and caches
- API endpoint configuration
- Processing options
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import os


def get_project_root() -> Path:
    """
    Get the project root directory.
    
    Traverses up from this file to find the project root,
    identified by the presence of README.md or pyproject.toml.
    
    Returns:
        Path to project root directory
    """
    current = Path(__file__).resolve().parent
    
    # Go up until we find project indicators
    for _ in range(10):  # Max 10 levels up
        if (current / "README.md").exists() or (current / "pyproject.toml").exists():
            return current
        current = current.parent
    
    # Fallback: assume src/utils/config.py structure
    return Path(__file__).resolve().parent.parent.parent


def get_default_paths() -> dict:
    """
    Get default paths relative to project root.
    
    Returns:
        Dictionary of default path configurations
    """
    root = get_project_root()
    
    return {
        # Input paths
        "input_csv": root / "resources" / "study_overview_examples" / "Yeshasvi_2_enriched.csv",
        "input_sheet1": root / "resources" / "study_overiew_work_needed" / "yeshasvi_sheet_1.csv",
        
        # Output paths
        "output_dir": root / "outputs",
        "enriched_csv_dir": root / "outputs" / "enriched_csv",
        "provenance_dir": root / "outputs" / "provenance_logs",
        "validation_dir": root / "outputs" / "validation_reports",
        "ml_json_dir": root / "outputs" / "ml_json",
        
        # Cache paths
        "cache_dir": root / "resources" / "osdr_api" / "raw",
        "isa_tab_dir": root / "resources" / "isa_tab",
        
        # Example/schema paths
        "examples_dir": root / "resources" / "study_overview_examples",
        "schema_dir": root / "resources" / "schema",
    }


@dataclass
class APIEndpoints:
    """Configuration for OSDR API endpoints."""
    
    # OSDR Biological Data API (Primary)
    biodata_api_base: str = "https://visualization.osdr.nasa.gov/biodata/api/v2"
    
    # OSDR Developer API (Fallback)
    developer_api_base: str = "https://osdr.nasa.gov/osdr/data"
    
    # GeneLab Files API (for ISA-Tab downloads)
    genelab_files_api: str = "https://genelab-data.ndc.nasa.gov/genelab/data/glds/files"
    
    # OSDR download base URL
    osdr_download_base: str = "https://osdr.nasa.gov"


@dataclass
class ProcessingOptions:
    """Configuration for processing behavior."""
    
    # Caching
    use_cache: bool = True
    clear_cache_on_start: bool = False
    
    # Fetching
    fetch_isa_tab: bool = True
    request_timeout: int = 60
    max_retries: int = 3
    
    # Enrichment
    overwrite_existing: bool = False  # CRITICAL: Must always be False for science safety
    require_provenance: bool = True
    log_conflicts: bool = True
    
    # Validation
    validate_against_schema: bool = True
    strict_mode: bool = False  # Fail on warnings if True


@dataclass
class Config:
    """
    Main configuration class for the OSDR Metadata Intelligence Engine.
    
    Combines path configuration, API endpoints, and processing options.
    """
    
    # Paths (auto-populated from defaults if not provided)
    project_root: Path = field(default_factory=get_project_root)
    input_csv: Optional[Path] = None
    output_dir: Optional[Path] = None
    cache_dir: Optional[Path] = None
    isa_tab_dir: Optional[Path] = None
    
    # Endpoints
    endpoints: APIEndpoints = field(default_factory=APIEndpoints)
    
    # Options
    options: ProcessingOptions = field(default_factory=ProcessingOptions)
    
    def __post_init__(self):
        """Set default paths after initialization."""
        defaults = get_default_paths()
        
        if self.input_csv is None:
            self.input_csv = defaults["input_csv"]
        if self.output_dir is None:
            self.output_dir = defaults["output_dir"]
        if self.cache_dir is None:
            self.cache_dir = defaults["cache_dir"]
        if self.isa_tab_dir is None:
            self.isa_tab_dir = defaults["isa_tab_dir"]
    
    @property
    def enriched_csv_dir(self) -> Path:
        """Get enriched CSV output directory."""
        return self.output_dir / "enriched_csv"
    
    @property
    def provenance_dir(self) -> Path:
        """Get provenance logs directory."""
        return self.output_dir / "provenance_logs"
    
    @property
    def validation_dir(self) -> Path:
        """Get validation reports directory."""
        return self.output_dir / "validation_reports"
    
    @property
    def ml_json_dir(self) -> Path:
        """Get ML JSON output directory."""
        return self.output_dir / "ml_json"
    
    def ensure_output_dirs(self) -> None:
        """Create all output directories if they don't exist."""
        for dir_path in [
            self.output_dir,
            self.enriched_csv_dir,
            self.provenance_dir,
            self.validation_dir,
            self.ml_json_dir,
        ]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def from_env(cls) -> "Config":
        """
        Create configuration from environment variables.
        
        Supported environment variables:
        - OSDR_PROJECT_ROOT: Project root directory
        - OSDR_INPUT_CSV: Input CSV path
        - OSDR_OUTPUT_DIR: Output directory
        - OSDR_CACHE_DIR: Cache directory
        - OSDR_USE_CACHE: Whether to use cache (true/false)
        
        Returns:
            Config instance
        """
        config = cls()
        
        if os.getenv("OSDR_PROJECT_ROOT"):
            config.project_root = Path(os.getenv("OSDR_PROJECT_ROOT"))
        
        if os.getenv("OSDR_INPUT_CSV"):
            config.input_csv = Path(os.getenv("OSDR_INPUT_CSV"))
        
        if os.getenv("OSDR_OUTPUT_DIR"):
            config.output_dir = Path(os.getenv("OSDR_OUTPUT_DIR"))
        
        if os.getenv("OSDR_CACHE_DIR"):
            config.cache_dir = Path(os.getenv("OSDR_CACHE_DIR"))
        
        if os.getenv("OSDR_USE_CACHE"):
            config.options.use_cache = os.getenv("OSDR_USE_CACHE").lower() in ("true", "1", "yes")
        
        return config

