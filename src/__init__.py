"""
NASA OSDR Metadata Intelligence Engine

A science-grade metadata enrichment pipeline for NASA's Open Science
Data Repository (OSDR) multi-omics studies.

Modules:
- core: Core enrichment engine (API client, ISA parser, enrichment rules)
- intelligence: AI-assisted and rule-based inference
- validation: Schema validation and conflict detection
- utils: Configuration, logging, and file utilities
- ml_builder: ML-ready output generation (future)
- dashboard: Interactive visualization (future)

Key principles:
1. Never overwrite existing data
2. Never hallucinate - only fill when evidence exists
3. Track provenance for every enrichment
4. Deterministic and reproducible
"""

__version__ = "1.0.0"
__author__ = "NASA OSDR Metadata Team"

from src.core.pipeline import run_pipeline, PipelineConfig
from src.core.provenance import ProvenanceTracker
from src.core.osdr_client import OSDRClient

__all__ = [
    "run_pipeline",
    "PipelineConfig",
    "ProvenanceTracker",
    "OSDRClient",
]

