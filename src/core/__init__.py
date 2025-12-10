"""
NASA OSDR Metadata Intelligence Engine - Core Module

This module provides the core functionality for science-grade metadata
enrichment of NASA Open Science Data Repository (OSDR) multi-omics studies.

Key components:
- osdr_client: API client for OSDR Biological Data API and ISA-Tab downloads
- isa_parser: Parser for ISA-Tab Study and Assay files
- enrichment_rules: Deterministic biological enrichment rules
- provenance: Provenance tracking data structures
- pipeline: Orchestration over CSV datasets
- constants: Controlled vocabularies and enums
"""

from src.core.constants import (
    CONTROLLED_STRAINS,
    CONTROLLED_TISSUES,
    CONTROLLED_SEX_VALUES,
    CONTROLLED_ASSAY_TYPES,
    RODENT_ORGANISMS,
    ProvenanceSource,
    ConfidenceLevel,
)
from src.core.provenance import ProvenanceEntry, ConflictEntry, ProvenanceTracker
from src.core.osdr_client import OSDRClient
from src.core.isa_parser import ISAParser
from src.core.enrichment_rules import enrich_row, EnrichmentResult
from src.core.pipeline import run_pipeline, PipelineConfig

__all__ = [
    # Constants
    "CONTROLLED_STRAINS",
    "CONTROLLED_TISSUES",
    "CONTROLLED_SEX_VALUES",
    "CONTROLLED_ASSAY_TYPES",
    "RODENT_ORGANISMS",
    "ProvenanceSource",
    "ConfidenceLevel",
    # Provenance
    "ProvenanceEntry",
    "ConflictEntry",
    "ProvenanceTracker",
    # Client
    "OSDRClient",
    # Parser
    "ISAParser",
    # Enrichment
    "enrich_row",
    "EnrichmentResult",
    # Pipeline
    "run_pipeline",
    "PipelineConfig",
]

