"""
NASA OSDR Metadata Intelligence Engine - Schema Validator

This module provides validation against controlled vocabularies
to ensure metadata values conform to NASA/scientific standards.

Validation is non-blocking by default - issues are collected
and reported, not raised as exceptions.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from src.core.constants import (
    CONTROLLED_STRAINS,
    CONTROLLED_TISSUES,
    CONTROLLED_SEX_VALUES,
    CONTROLLED_ASSAY_TYPES,
    MOUSE_STRAIN_PATTERNS,
    RODENT_ORGANISMS,
)


class ValidationSeverity(str, Enum):
    """Severity levels for validation issues."""
    ERROR = "error"       # Must be fixed
    WARNING = "warning"   # Should be reviewed
    INFO = "info"         # For information only


@dataclass
class ValidationIssue:
    """A single validation issue."""
    field_name: str
    value: str
    message: str
    severity: ValidationSeverity
    suggestion: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "field_name": self.field_name,
            "value": self.value,
            "message": self.message,
            "severity": self.severity.value,
            "suggestion": self.suggestion,
        }


@dataclass
class ValidationResult:
    """Result of validating a row or field."""
    is_valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    
    @property
    def has_errors(self) -> bool:
        """Check if there are any error-level issues."""
        return any(i.severity == ValidationSeverity.ERROR for i in self.issues)
    
    @property
    def has_warnings(self) -> bool:
        """Check if there are any warning-level issues."""
        return any(i.severity == ValidationSeverity.WARNING for i in self.issues)
    
    def add_issue(
        self,
        field_name: str,
        value: str,
        message: str,
        severity: ValidationSeverity = ValidationSeverity.WARNING,
        suggestion: Optional[str] = None,
    ) -> None:
        """Add a validation issue."""
        self.issues.append(ValidationIssue(
            field_name=field_name,
            value=value,
            message=message,
            severity=severity,
            suggestion=suggestion,
        ))
        
        if severity == ValidationSeverity.ERROR:
            self.is_valid = False
    
    def merge(self, other: "ValidationResult") -> None:
        """Merge another result into this one."""
        self.issues.extend(other.issues)
        if not other.is_valid:
            self.is_valid = False


def validate_strain(value: str) -> ValidationResult:
    """
    Validate a mouse strain value against controlled vocabulary.
    
    Args:
        value: The strain value to validate
        
    Returns:
        ValidationResult with any issues found
    """
    result = ValidationResult(is_valid=True)
    
    if not value or value.lower() in ["n/a", "not applicable"]:
        return result
    
    value_lower = value.lower().strip()
    
    # Check if in controlled vocabulary
    if value_lower in CONTROLLED_STRAINS:
        return result
    
    # Check if matches any known pattern
    value_upper = value.upper()
    for pattern in MOUSE_STRAIN_PATTERNS:
        if pattern.upper() in value_upper:
            # Valid pattern, but not normalized
            normalized = CONTROLLED_STRAINS.get(value_lower, value)
            if normalized != value:
                result.add_issue(
                    field_name="mouse_strain",
                    value=value,
                    message=f"Strain value should be normalized",
                    severity=ValidationSeverity.INFO,
                    suggestion=normalized,
                )
            return result
    
    # Unknown strain
    result.add_issue(
        field_name="mouse_strain",
        value=value,
        message=f"Unknown strain '{value}' not in controlled vocabulary",
        severity=ValidationSeverity.WARNING,
    )
    
    return result


def validate_sex(value: str) -> ValidationResult:
    """
    Validate a sex value against controlled vocabulary.
    
    Args:
        value: The sex value to validate
        
    Returns:
        ValidationResult with any issues found
    """
    result = ValidationResult(is_valid=True)
    
    if not value or value.lower() in ["n/a", "not applicable"]:
        return result
    
    value_lower = value.lower().strip()
    
    # Check if in controlled vocabulary
    if value_lower in CONTROLLED_SEX_VALUES:
        normalized = CONTROLLED_SEX_VALUES[value_lower]
        if normalized != value:
            result.add_issue(
                field_name="mouse_sex",
                value=value,
                message="Sex value should be normalized",
                severity=ValidationSeverity.INFO,
                suggestion=normalized,
            )
        return result
    
    # Check normalized values
    if value in CONTROLLED_SEX_VALUES.values():
        return result
    
    # Unknown value
    result.add_issue(
        field_name="mouse_sex",
        value=value,
        message=f"Unknown sex value '{value}'",
        severity=ValidationSeverity.WARNING,
        suggestion="Male, Female, or Mixed",
    )
    
    return result


def validate_tissue(value: str) -> ValidationResult:
    """
    Validate a tissue/organ value against controlled vocabulary.
    
    Args:
        value: The tissue value to validate
        
    Returns:
        ValidationResult with any issues found
    """
    result = ValidationResult(is_valid=True)
    
    if not value or value.lower() in ["n/a", "not applicable"]:
        return result
    
    value_lower = value.lower().strip()
    
    # Check if in controlled vocabulary
    if value_lower in CONTROLLED_TISSUES:
        normalized = CONTROLLED_TISSUES[value_lower]
        if normalized != value:
            result.add_issue(
                field_name="organ_sampled",
                value=value,
                message="Tissue value should be normalized",
                severity=ValidationSeverity.INFO,
                suggestion=normalized,
            )
        return result
    
    # Check normalized values
    if value in CONTROLLED_TISSUES.values():
        return result
    
    # Unknown tissue - this is common and okay
    result.add_issue(
        field_name="organ_sampled",
        value=value,
        message=f"Tissue '{value}' not in controlled vocabulary",
        severity=ValidationSeverity.INFO,
    )
    
    return result


def validate_assay_type(value: str) -> ValidationResult:
    """
    Validate an assay type value against controlled vocabulary.
    
    Args:
        value: The assay type value to validate
        
    Returns:
        ValidationResult with any issues found
    """
    result = ValidationResult(is_valid=True)
    
    if not value:
        return result
    
    # Handle comma-separated list
    assay_values = [v.strip() for v in value.split(",")]
    
    for assay in assay_values:
        assay_lower = assay.lower().strip()
        
        # Check if in controlled vocabulary
        if assay_lower in CONTROLLED_ASSAY_TYPES:
            normalized = CONTROLLED_ASSAY_TYPES[assay_lower]
            if normalized != assay:
                result.add_issue(
                    field_name="assay_on_organ",
                    value=assay,
                    message="Assay type should be normalized",
                    severity=ValidationSeverity.INFO,
                    suggestion=normalized,
                )
            continue
        
        # Check normalized values
        if assay in CONTROLLED_ASSAY_TYPES.values():
            continue
        
        # Unknown assay type
        result.add_issue(
            field_name="assay_on_organ",
            value=assay,
            message=f"Unknown assay type '{assay}'",
            severity=ValidationSeverity.INFO,
        )
    
    return result


def validate_row(
    row: Dict[str, Any],
    required_fields: Optional[List[str]] = None,
) -> ValidationResult:
    """
    Validate an entire row against schema.
    
    Args:
        row: Row dictionary to validate
        required_fields: Optional list of required fields
        
    Returns:
        ValidationResult with all issues found
    """
    result = ValidationResult(is_valid=True)
    
    # Check required fields
    if required_fields:
        for field_name in required_fields:
            value = row.get(field_name, "")
            if not value or not str(value).strip():
                result.add_issue(
                    field_name=field_name,
                    value="",
                    message=f"Required field '{field_name}' is empty",
                    severity=ValidationSeverity.WARNING,
                )
    
    # Validate specific fields
    if "mouse_strain" in row:
        result.merge(validate_strain(str(row["mouse_strain"])))
    
    if "mouse_sex" in row:
        result.merge(validate_sex(str(row["mouse_sex"])))
    
    if "organ_sampled" in row:
        result.merge(validate_tissue(str(row["organ_sampled"])))
    
    if "assay_on_organ" in row:
        result.merge(validate_assay_type(str(row["assay_on_organ"])))
    
    return result


class SchemaValidator:
    """
    Schema validator for CSV rows.
    
    Validates rows against controlled vocabularies and
    required field constraints.
    """
    
    def __init__(
        self,
        required_fields: Optional[List[str]] = None,
        strict_mode: bool = False,
    ):
        """
        Initialize schema validator.
        
        Args:
            required_fields: List of required field names
            strict_mode: If True, warnings are treated as errors
        """
        self.required_fields = required_fields or []
        self.strict_mode = strict_mode
        self._all_issues: List[ValidationIssue] = []
    
    def validate(self, row: Dict[str, Any]) -> ValidationResult:
        """
        Validate a single row.
        
        Args:
            row: Row dictionary to validate
            
        Returns:
            ValidationResult
        """
        result = validate_row(row, self.required_fields)
        
        # In strict mode, upgrade warnings to errors
        if self.strict_mode:
            for issue in result.issues:
                if issue.severity == ValidationSeverity.WARNING:
                    issue.severity = ValidationSeverity.ERROR
                    result.is_valid = False
        
        # Collect all issues
        self._all_issues.extend(result.issues)
        
        return result
    
    def validate_all(
        self,
        rows: List[Dict[str, Any]],
    ) -> List[ValidationResult]:
        """
        Validate all rows.
        
        Args:
            rows: List of row dictionaries
            
        Returns:
            List of ValidationResults
        """
        return [self.validate(row) for row in rows]
    
    @property
    def all_issues(self) -> List[ValidationIssue]:
        """Get all issues from all validations."""
        return self._all_issues
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all validation issues."""
        errors = sum(1 for i in self._all_issues if i.severity == ValidationSeverity.ERROR)
        warnings = sum(1 for i in self._all_issues if i.severity == ValidationSeverity.WARNING)
        info = sum(1 for i in self._all_issues if i.severity == ValidationSeverity.INFO)
        
        # Group by field
        by_field: Dict[str, int] = {}
        for issue in self._all_issues:
            by_field[issue.field_name] = by_field.get(issue.field_name, 0) + 1
        
        return {
            "total_issues": len(self._all_issues),
            "errors": errors,
            "warnings": warnings,
            "info": info,
            "by_field": by_field,
        }

