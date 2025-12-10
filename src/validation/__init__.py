"""
NASA OSDR Metadata Intelligence Engine - Validation Module

This module provides validation and consistency checking for metadata,
including schema validation, conflict detection, and replicate analysis.

All validation is non-blocking - issues are logged as warnings,
not raised as exceptions (unless in strict mode).
"""

from src.validation.schema_validator import (
    SchemaValidator,
    ValidationResult,
    validate_row,
    validate_strain,
    validate_sex,
    validate_tissue,
    validate_assay_type,
)
from src.validation.conflict_checker import (
    ConflictChecker,
    detect_conflicts,
    ConflictReport,
)

__all__ = [
    # Schema validation
    "SchemaValidator",
    "ValidationResult",
    "validate_row",
    "validate_strain",
    "validate_sex",
    "validate_tissue",
    "validate_assay_type",
    # Conflict checking
    "ConflictChecker",
    "detect_conflicts",
    "ConflictReport",
]

