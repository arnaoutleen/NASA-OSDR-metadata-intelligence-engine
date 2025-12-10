"""
NASA OSDR Metadata Intelligence Engine - Conflict Checker

This module detects and records conflicts between different data sources
(API, ISA-Tab, sample name inference) for the same metadata field.

When conflicts are detected, they are logged rather than resolved
automatically, preserving scientific integrity.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from src.core.provenance import ConflictEntry


@dataclass
class ConflictReport:
    """Summary report of all detected conflicts."""
    
    total_conflicts: int = 0
    conflicts_by_field: Dict[str, int] = field(default_factory=dict)
    conflicts_by_study: Dict[str, int] = field(default_factory=dict)
    conflict_entries: List[ConflictEntry] = field(default_factory=list)
    
    def add_conflict(self, conflict: ConflictEntry) -> None:
        """Add a conflict to the report."""
        self.conflict_entries.append(conflict)
        self.total_conflicts += 1
        
        # Update field counts
        field_name = conflict.field_name
        self.conflicts_by_field[field_name] = self.conflicts_by_field.get(field_name, 0) + 1
        
        # Update study counts
        osd_id = conflict.osd_id
        self.conflicts_by_study[osd_id] = self.conflicts_by_study.get(osd_id, 0) + 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "total_conflicts": self.total_conflicts,
            "conflicts_by_field": self.conflicts_by_field,
            "conflicts_by_study": self.conflicts_by_study,
            "conflicts": [c.to_dict() for c in self.conflict_entries],
        }


class ConflictChecker:
    """
    Detects conflicts between different data sources for metadata fields.
    
    Data sources checked:
    - OSDR Biological Data API
    - OSDR Developer API
    - ISA-Tab Study file
    - ISA-Tab Assay file
    - Sample name inference
    
    When values from different sources disagree, a conflict is recorded
    rather than making an arbitrary choice.
    """
    
    def __init__(self):
        """Initialize conflict checker."""
        self.report = ConflictReport()
    
    def check_field(
        self,
        osd_id: str,
        sample_id: str,
        field_name: str,
        values: Dict[str, Any],
        tolerance: float = 0.0,
    ) -> Optional[ConflictEntry]:
        """
        Check if values from different sources conflict.
        
        Args:
            osd_id: The OSD identifier
            sample_id: The sample identifier
            field_name: The field being checked
            values: Dict mapping source_name -> value
            tolerance: For numeric fields, allowed difference (default: 0)
            
        Returns:
            ConflictEntry if conflict detected, None otherwise
        """
        # Filter out empty values
        non_empty = {
            k: v for k, v in values.items()
            if v is not None and str(v).strip()
        }
        
        if len(non_empty) <= 1:
            # No conflict possible with 0 or 1 values
            return None
        
        # Check for conflicts
        unique_values = self._get_unique_values(non_empty, tolerance)
        
        if len(unique_values) <= 1:
            # All values are equivalent
            return None
        
        # Conflict detected
        conflict = ConflictEntry(
            osd_id=osd_id,
            sample_id=sample_id,
            field_name=field_name,
            conflicting_values=non_empty,
            resolution="unresolved",
            notes=f"Found {len(unique_values)} distinct values",
        )
        
        self.report.add_conflict(conflict)
        return conflict
    
    def _get_unique_values(
        self,
        values: Dict[str, Any],
        tolerance: float,
    ) -> Set[str]:
        """Get unique values, accounting for tolerance."""
        unique = set()
        
        for value in values.values():
            normalized = self._normalize_value(value)
            
            # For numeric comparison with tolerance
            if tolerance > 0:
                try:
                    num = float(normalized)
                    # Check if close to any existing value
                    found_match = False
                    for existing in list(unique):
                        try:
                            existing_num = float(existing)
                            if abs(num - existing_num) <= tolerance:
                                found_match = True
                                break
                        except ValueError:
                            pass
                    
                    if not found_match:
                        unique.add(normalized)
                except ValueError:
                    unique.add(normalized)
            else:
                unique.add(normalized)
        
        return unique
    
    def _normalize_value(self, value: Any) -> str:
        """Normalize a value for comparison."""
        if value is None:
            return ""
        
        s = str(value).strip().lower()
        
        # Normalize common variations
        normalizations = {
            "male": "male",
            "m": "male",
            "female": "female",
            "f": "female",
            "yes": "yes",
            "y": "yes",
            "no": "no",
            "n": "no",
            "true": "yes",
            "false": "no",
        }
        
        return normalizations.get(s, s)
    
    def check_sample(
        self,
        osd_id: str,
        sample_id: str,
        api_data: Dict[str, Any],
        isa_data: Dict[str, Any],
        inferred_data: Optional[Dict[str, Any]] = None,
    ) -> List[ConflictEntry]:
        """
        Check all fields for a sample across data sources.
        
        Args:
            osd_id: The OSD identifier
            sample_id: The sample identifier
            api_data: Data from OSDR API
            isa_data: Data from ISA-Tab
            inferred_data: Optional data from sample name inference
            
        Returns:
            List of detected conflicts
        """
        conflicts = []
        
        # Fields to check
        fields_to_check = [
            "strain", "sex", "age", "material_type",
            "organism", "organism_part",
        ]
        
        for field_name in fields_to_check:
            values = {}
            
            if field_name in api_data:
                values["api"] = api_data[field_name]
            
            if field_name in isa_data:
                values["isa_tab"] = isa_data[field_name]
            
            if inferred_data and field_name in inferred_data:
                values["inferred"] = inferred_data[field_name]
            
            conflict = self.check_field(osd_id, sample_id, field_name, values)
            if conflict:
                conflicts.append(conflict)
        
        return conflicts
    
    def check_factor_values(
        self,
        osd_id: str,
        sample_id: str,
        api_factors: Dict[str, str],
        isa_factors: Dict[str, str],
    ) -> List[ConflictEntry]:
        """
        Check factor values across sources.
        
        Args:
            osd_id: The OSD identifier
            sample_id: The sample identifier
            api_factors: Factor values from API
            isa_factors: Factor values from ISA-Tab
            
        Returns:
            List of detected conflicts
        """
        conflicts = []
        
        # Get all factor names from both sources
        all_factors = set(api_factors.keys()) | set(isa_factors.keys())
        
        for factor_name in all_factors:
            values = {}
            
            if factor_name in api_factors:
                values["api"] = api_factors[factor_name]
            
            if factor_name in isa_factors:
                values["isa_tab"] = isa_factors[factor_name]
            
            # Normalize factor name for field_name
            field_name = f"factor:{factor_name}"
            
            conflict = self.check_field(osd_id, sample_id, field_name, values)
            if conflict:
                conflicts.append(conflict)
        
        return conflicts
    
    def get_report(self) -> ConflictReport:
        """Get the conflict report."""
        return self.report
    
    def reset(self) -> None:
        """Reset the conflict checker."""
        self.report = ConflictReport()


def detect_conflicts(
    osd_id: str,
    sample_id: str,
    api_data: Dict[str, Any],
    isa_data: Dict[str, Any],
    inferred_data: Optional[Dict[str, Any]] = None,
) -> List[ConflictEntry]:
    """
    Convenience function to detect conflicts for a sample.
    
    Args:
        osd_id: The OSD identifier
        sample_id: The sample identifier
        api_data: Data from OSDR API
        isa_data: Data from ISA-Tab
        inferred_data: Optional data from sample name inference
        
    Returns:
        List of detected conflicts
    """
    checker = ConflictChecker()
    return checker.check_sample(osd_id, sample_id, api_data, isa_data, inferred_data)

