"""
NASA OSDR Metadata Intelligence Engine - Biological Rules

This module applies biological consistency rules to validate
and potentially correct metadata values.

Rules are based on biological knowledge and NASA OSDR conventions.
All suggestions require explicit acceptance before use.

CRITICAL: These are SANITY CHECKS, not data sources.
We only flag inconsistencies - we never fabricate data.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
import re

from src.core.constants import (
    RODENT_ORGANISMS,
    CONTROLLED_STRAINS,
    CONTROLLED_TISSUES,
    ConfidenceLevel,
)


@dataclass
class BiologicalIssue:
    """A potential biological inconsistency."""
    field_name: str
    current_value: str
    issue_type: str
    description: str
    suggestion: Optional[str] = None
    confidence: ConfidenceLevel = ConfidenceLevel.MEDIUM
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "field_name": self.field_name,
            "current_value": self.current_value,
            "issue_type": self.issue_type,
            "description": self.description,
            "suggestion": self.suggestion,
            "confidence": self.confidence.value,
        }


@dataclass
class BiologicalCheckResult:
    """Result of biological consistency checking."""
    sample_id: str
    is_consistent: bool
    issues: List[BiologicalIssue] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sample_id": self.sample_id,
            "is_consistent": self.is_consistent,
            "issues": [i.to_dict() for i in self.issues],
        }


class BiologicalRuleEngine:
    """
    Engine for applying biological consistency rules.
    
    Rules include:
    - Organism-tissue compatibility (mouse strains with valid tissues)
    - Age plausibility (reasonable age ranges for organisms)
    - Treatment-timepoint consistency
    - Spaceflight/ground group logic
    """
    
    # Valid tissues for rodent studies
    RODENT_TISSUES: Set[str] = {
        "brain", "heart", "liver", "kidney", "spleen", "lung",
        "skeletal muscle", "soleus", "gastrocnemius", "tibialis anterior",
        "quadriceps", "bone marrow", "femur", "tibia", "bone",
        "adrenal", "thymus", "pancreas", "colon", "small intestine",
        "eye", "retina", "skin", "adipose tissue", "blood", "plasma",
        "testis", "ovary", "uterus",
    }
    
    # Age ranges by organism (in weeks)
    AGE_RANGES = {
        "Mus musculus": {"min": 0, "max": 156},  # ~3 years max
        "Rattus norvegicus": {"min": 0, "max": 208},  # ~4 years max
    }
    
    # Common age ranges for experiments (in weeks)
    TYPICAL_EXPERIMENT_AGES = {
        "Mus musculus": {"min": 6, "max": 52},  # 6 weeks to 1 year
        "Rattus norvegicus": {"min": 6, "max": 52},
    }
    
    def __init__(self):
        """Initialize biological rule engine."""
        pass
    
    def check_sample(
        self,
        sample_id: str,
        sample_data: Dict[str, Any],
        study_metadata: Optional[Dict[str, Any]] = None,
    ) -> BiologicalCheckResult:
        """
        Check a sample for biological consistency.
        
        Args:
            sample_id: The sample identifier
            sample_data: Sample metadata dictionary
            study_metadata: Optional study-level metadata
            
        Returns:
            BiologicalCheckResult with any issues found
        """
        result = BiologicalCheckResult(sample_id=sample_id, is_consistent=True)
        
        # Get organism
        organism = sample_data.get("organism", "")
        if not organism and study_metadata:
            organism = study_metadata.get("organism", "")
        
        # Check strain-organism consistency
        strain = sample_data.get("strain", sample_data.get("mouse_strain", ""))
        if strain:
            issue = self._check_strain_organism(strain, organism)
            if issue:
                result.issues.append(issue)
                result.is_consistent = False
        
        # Check tissue validity
        tissue = sample_data.get("material_type", sample_data.get("organ_sampled", ""))
        if tissue:
            issue = self._check_tissue_validity(tissue, organism)
            if issue:
                result.issues.append(issue)
        
        # Check age plausibility
        age = sample_data.get("age", sample_data.get("age_when_sent_to_space", ""))
        if age:
            issue = self._check_age_plausibility(age, organism)
            if issue:
                result.issues.append(issue)
        
        # Check sex value
        sex = sample_data.get("sex", sample_data.get("mouse_sex", ""))
        if sex:
            issue = self._check_sex_value(sex)
            if issue:
                result.issues.append(issue)
        
        # Check spaceflight logic
        space_ground = sample_data.get("space_or_ground", "")
        factor_values = sample_data.get("factor_values", {})
        if space_ground or factor_values:
            issue = self._check_spaceflight_logic(space_ground, factor_values)
            if issue:
                result.issues.append(issue)
        
        return result
    
    def _check_strain_organism(
        self,
        strain: str,
        organism: str,
    ) -> Optional[BiologicalIssue]:
        """Check if strain is valid for organism."""
        strain_lower = strain.lower()
        organism_lower = organism.lower() if organism else ""
        
        # Check if it's a known mouse strain
        is_mouse_strain = any(
            pattern.lower() in strain_lower
            for pattern in ["c57bl", "balb", "129s", "fvb", "dba", "cd-1", "icr", "swiss"]
        )
        
        if is_mouse_strain:
            if organism and "mus musculus" not in organism_lower:
                return BiologicalIssue(
                    field_name="strain",
                    current_value=strain,
                    issue_type="strain_organism_mismatch",
                    description=f"Mouse strain '{strain}' but organism is '{organism}'",
                    suggestion="Verify organism is Mus musculus",
                    confidence=ConfidenceLevel.HIGH,
                )
        
        # Check if it's a known rat strain
        is_rat_strain = any(
            pattern.lower() in strain_lower
            for pattern in ["sprague", "wistar", "long evans", "fischer", "lewis"]
        )
        
        if is_rat_strain:
            if organism and "rattus" not in organism_lower:
                return BiologicalIssue(
                    field_name="strain",
                    current_value=strain,
                    issue_type="strain_organism_mismatch",
                    description=f"Rat strain '{strain}' but organism is '{organism}'",
                    suggestion="Verify organism is Rattus norvegicus",
                    confidence=ConfidenceLevel.HIGH,
                )
        
        return None
    
    def _check_tissue_validity(
        self,
        tissue: str,
        organism: str,
    ) -> Optional[BiologicalIssue]:
        """Check if tissue is biologically plausible."""
        tissue_lower = tissue.lower()
        
        # Check for cell line indicators (not actual tissue)
        cell_line_indicators = ["cell", "line", "culture", "-1", "-2", "hela", "hek"]
        if any(ind in tissue_lower for ind in cell_line_indicators):
            return BiologicalIssue(
                field_name="tissue",
                current_value=tissue,
                issue_type="cell_line_as_tissue",
                description=f"'{tissue}' appears to be a cell line, not a tissue",
                confidence=ConfidenceLevel.MEDIUM,
            )
        
        # For rodents, check if tissue is in known list
        organism_lower = organism.lower() if organism else ""
        if any(r.lower() in organism_lower for r in RODENT_ORGANISMS):
            # Normalize and check
            is_known_tissue = any(
                known in tissue_lower
                for known in self.RODENT_TISSUES
            )
            
            if not is_known_tissue and tissue_lower not in ["n/a", "not applicable", ""]:
                return BiologicalIssue(
                    field_name="tissue",
                    current_value=tissue,
                    issue_type="unusual_tissue",
                    description=f"'{tissue}' is not a commonly recognized rodent tissue",
                    confidence=ConfidenceLevel.LOW,
                )
        
        return None
    
    def _check_age_plausibility(
        self,
        age: str,
        organism: str,
    ) -> Optional[BiologicalIssue]:
        """Check if age is biologically plausible."""
        # Extract numeric value and unit
        match = re.search(r'(\d+\.?\d*)\s*(weeks?|days?|months?|years?)?', age, re.IGNORECASE)
        if not match:
            return None
        
        value = float(match.group(1))
        unit = (match.group(2) or "").lower()
        
        # Convert to weeks for comparison
        if unit.startswith("day"):
            value_weeks = value / 7
        elif unit.startswith("month"):
            value_weeks = value * 4.3
        elif unit.startswith("year"):
            value_weeks = value * 52
        else:
            value_weeks = value  # Assume weeks
        
        organism_lower = organism.lower() if organism else ""
        
        # Check against known ranges
        for org_name, ranges in self.AGE_RANGES.items():
            if org_name.lower() in organism_lower:
                if value_weeks < ranges["min"] or value_weeks > ranges["max"]:
                    return BiologicalIssue(
                        field_name="age",
                        current_value=age,
                        issue_type="implausible_age",
                        description=f"Age {age} (~{value_weeks:.1f} weeks) is outside expected range for {org_name}",
                        confidence=ConfidenceLevel.MEDIUM,
                    )
                
                # Check if outside typical experimental range
                typical = self.TYPICAL_EXPERIMENT_AGES.get(org_name)
                if typical:
                    if value_weeks < typical["min"] or value_weeks > typical["max"]:
                        return BiologicalIssue(
                            field_name="age",
                            current_value=age,
                            issue_type="unusual_age",
                            description=f"Age {age} is outside typical experimental range ({typical['min']}-{typical['max']} weeks)",
                            confidence=ConfidenceLevel.LOW,
                        )
        
        return None
    
    def _check_sex_value(self, sex: str) -> Optional[BiologicalIssue]:
        """Check if sex value is valid."""
        sex_lower = sex.lower().strip()
        
        valid_values = ["male", "female", "m", "f", "mixed", "both", "not applicable", "n/a", "unknown"]
        
        if sex_lower not in valid_values:
            return BiologicalIssue(
                field_name="sex",
                current_value=sex,
                issue_type="invalid_sex",
                description=f"'{sex}' is not a recognized sex value",
                suggestion="Use Male, Female, or Mixed",
                confidence=ConfidenceLevel.HIGH,
            )
        
        return None
    
    def _check_spaceflight_logic(
        self,
        space_ground: str,
        factor_values: Dict[str, str],
    ) -> Optional[BiologicalIssue]:
        """Check spaceflight/ground control logic."""
        space_ground_lower = space_ground.lower() if space_ground else ""
        
        # Check for contradictory factor values
        spaceflight_factor = None
        for key, val in factor_values.items():
            if "spaceflight" in key.lower() or "flight" in key.lower():
                spaceflight_factor = val
                break
        
        if spaceflight_factor and space_ground:
            sf_lower = spaceflight_factor.lower()
            
            # Check for contradictions
            if "flight" in sf_lower and "ground" in space_ground_lower:
                return BiologicalIssue(
                    field_name="space_or_ground",
                    current_value=space_ground,
                    issue_type="spaceflight_contradiction",
                    description=f"space_or_ground='{space_ground}' but factor value indicates Flight",
                    confidence=ConfidenceLevel.MEDIUM,
                )
            
            if "ground" in sf_lower and "space" in space_ground_lower and "space" not in sf_lower:
                return BiologicalIssue(
                    field_name="space_or_ground",
                    current_value=space_ground,
                    issue_type="spaceflight_contradiction",
                    description=f"space_or_ground='{space_ground}' but factor value indicates Ground",
                    confidence=ConfidenceLevel.MEDIUM,
                )
        
        return None


def apply_biological_rules(
    sample_data: Dict[str, Any],
    study_metadata: Optional[Dict[str, Any]] = None,
) -> List[BiologicalIssue]:
    """
    Apply biological rules to a sample.
    
    Args:
        sample_data: Sample metadata dictionary
        study_metadata: Optional study-level metadata
        
    Returns:
        List of BiologicalIssue found
    """
    engine = BiologicalRuleEngine()
    sample_id = sample_data.get("id", sample_data.get("sample_id", "unknown"))
    result = engine.check_sample(sample_id, sample_data, study_metadata)
    return result.issues


def check_biological_consistency(
    samples: List[Dict[str, Any]],
    study_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, BiologicalCheckResult]:
    """
    Check biological consistency for multiple samples.
    
    Args:
        samples: List of sample metadata dictionaries
        study_metadata: Optional study-level metadata
        
    Returns:
        Dict mapping sample_id -> BiologicalCheckResult
    """
    engine = BiologicalRuleEngine()
    results = {}
    
    for sample in samples:
        sample_id = sample.get("id", sample.get("sample_id", "unknown"))
        results[sample_id] = engine.check_sample(sample_id, sample, study_metadata)
    
    return results

