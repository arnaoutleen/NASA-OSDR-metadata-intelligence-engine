"""
NASA OSDR Metadata Intelligence Engine - Unit Inference

This module infers units for numeric values (age, time, dose)
using contextual clues from field names, surrounding data, and patterns.

All inferences are returned as SUGGESTIONS with confidence levels,
not applied directly to metadata.

CRITICAL: Never assume units without context. A bare "10" could be:
- 10 weeks (age)
- 10 days (timepoint)
- 10 months (developmental stage)
- 10 Gy (radiation dose)
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import re

from src.core.constants import ConfidenceLevel


@dataclass
class UnitSuggestion:
    """A suggested unit for a numeric value."""
    original_value: str
    suggested_value: str
    unit: str
    confidence: ConfidenceLevel
    reasoning: str
    context_used: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "original_value": self.original_value,
            "suggested_value": self.suggested_value,
            "unit": self.unit,
            "confidence": self.confidence.value,
            "reasoning": self.reasoning,
            "context_used": self.context_used,
        }


class UnitInferrer:
    """
    Infers units for numeric values using contextual clues.
    
    Context sources:
    - Field/column name (e.g., "Age (weeks)")
    - Study description
    - Other samples in the same study
    - Common patterns in NASA OSDR data
    """
    
    # Patterns that indicate units are already present
    HAS_UNITS_PATTERNS = [
        r'\d+\s*(days?|d)(\s|$)',
        r'\d+\s*(weeks?|wks?|w)(\s|$)',
        r'\d+\s*(months?|mo)(\s|$)',
        r'\d+\s*(years?|yr)(\s|$)',
        r'\d+\s*(hours?|hrs?|h)(\s|$)',
        r'\d+\s*(minutes?|min)(\s|$)',
        r'\d+\s*Gy',
        r'\d+\s*cGy',
        r'\d+\s*mGy',
    ]
    
    # Context keywords that suggest specific units
    TIME_CONTEXT_KEYWORDS = {
        'days': ['day', 'daily', 'd post', 'after landing', 'post-flight'],
        'weeks': ['week', 'wk', 'weekly'],
        'months': ['month', 'mo ', 'monthly'],
        'hours': ['hour', 'hr', 'hourly'],
    }
    
    AGE_CONTEXT_KEYWORDS = {
        'weeks': ['week old', 'wk old', 'weeks of age', 'age (week', 'age[week'],
        'months': ['month old', 'mo old', 'months of age', 'age (month', 'age[month'],
        'days': ['day old', 'postnatal day', 'p', 'pnd'],
    }
    
    def __init__(self):
        """Initialize unit inferrer."""
        pass
    
    def infer_time_units(
        self,
        value: str,
        field_name: str = "",
        study_description: str = "",
    ) -> Optional[UnitSuggestion]:
        """
        Infer time units for a value.
        
        Args:
            value: The value to analyze
            field_name: Name of the field/column
            study_description: Study description for context
            
        Returns:
            UnitSuggestion if inference possible, None otherwise
        """
        value = str(value).strip()
        
        # Check if already has units
        if self._has_units(value):
            return None
        
        # Extract numeric part
        match = re.match(r'^(\d+\.?\d*)$', value)
        if not match:
            return None
        
        numeric = match.group(1)
        context = f"{field_name} {study_description}".lower()
        
        # Check for R+N or L-N patterns (spaceflight specific)
        r_match = re.match(r'^R\+?(\d+)$', value, re.IGNORECASE)
        if r_match:
            days = r_match.group(1)
            return UnitSuggestion(
                original_value=value,
                suggested_value=f"{days} days post-return",
                unit="days post-return",
                confidence=ConfidenceLevel.HIGH,
                reasoning="R+ pattern indicates days after mission return",
                context_used="pattern",
            )
        
        l_match = re.match(r'^L-?(\d+)$', value, re.IGNORECASE)
        if l_match:
            days = l_match.group(1)
            return UnitSuggestion(
                original_value=value,
                suggested_value=f"{days} days pre-launch",
                unit="days pre-launch",
                confidence=ConfidenceLevel.HIGH,
                reasoning="L- pattern indicates days before launch",
                context_used="pattern",
            )
        
        # Try to infer from context
        for unit, keywords in self.TIME_CONTEXT_KEYWORDS.items():
            for keyword in keywords:
                if keyword in context:
                    return UnitSuggestion(
                        original_value=value,
                        suggested_value=f"{numeric} {unit}",
                        unit=unit,
                        confidence=ConfidenceLevel.MEDIUM,
                        reasoning=f"Field/description contains '{keyword}'",
                        context_used=keyword,
                    )
        
        # No inference possible without context
        return None
    
    def infer_age_units(
        self,
        value: str,
        field_name: str = "",
        study_description: str = "",
        organism: str = "",
    ) -> Optional[UnitSuggestion]:
        """
        Infer age units for a value.
        
        Args:
            value: The value to analyze
            field_name: Name of the field/column
            study_description: Study description for context
            organism: Organism name (affects typical age units)
            
        Returns:
            UnitSuggestion if inference possible, None otherwise
        """
        value = str(value).strip()
        
        # Check if already has units
        if self._has_units(value):
            return None
        
        # Extract numeric part
        match = re.match(r'^(\d+\.?\d*)$', value)
        if not match:
            return None
        
        numeric = match.group(1)
        context = f"{field_name} {study_description}".lower()
        organism_lower = organism.lower()
        
        # Try to infer from context
        for unit, keywords in self.AGE_CONTEXT_KEYWORDS.items():
            for keyword in keywords:
                if keyword in context:
                    return UnitSuggestion(
                        original_value=value,
                        suggested_value=f"{numeric} {unit}",
                        unit=unit,
                        confidence=ConfidenceLevel.HIGH,
                        reasoning=f"Field/description contains '{keyword}'",
                        context_used=keyword,
                    )
        
        # Organism-specific defaults (LOW confidence)
        if "mouse" in organism_lower or "mus musculus" in organism_lower:
            # Mouse ages are typically in weeks for adults
            num = float(numeric)
            if 4 <= num <= 52:
                return UnitSuggestion(
                    original_value=value,
                    suggested_value=f"{numeric} weeks",
                    unit="weeks",
                    confidence=ConfidenceLevel.LOW,
                    reasoning="Mouse age typically in weeks (4-52 range)",
                    context_used="organism",
                )
        
        # No inference possible
        return None
    
    def infer_dose_units(
        self,
        value: str,
        field_name: str = "",
        study_description: str = "",
    ) -> Optional[UnitSuggestion]:
        """
        Infer radiation dose units for a value.
        
        Args:
            value: The value to analyze
            field_name: Name of the field/column
            study_description: Study description for context
            
        Returns:
            UnitSuggestion if inference possible, None otherwise
        """
        value = str(value).strip()
        
        # Check if already has units
        if self._has_units(value):
            return None
        
        # Extract numeric part
        match = re.match(r'^(\d+\.?\d*)$', value)
        if not match:
            return None
        
        numeric = match.group(1)
        context = f"{field_name} {study_description}".lower()
        
        # Check for radiation context
        radiation_keywords = ['radiation', 'irradiation', 'dose', 'gray', 'gy']
        
        if any(kw in context for kw in radiation_keywords):
            # Typical doses are in Gy or cGy
            num = float(numeric)
            if num <= 10:
                return UnitSuggestion(
                    original_value=value,
                    suggested_value=f"{numeric} Gy",
                    unit="Gy",
                    confidence=ConfidenceLevel.MEDIUM,
                    reasoning="Radiation context, typical whole-body dose range",
                    context_used="radiation field",
                )
            elif num > 10:
                return UnitSuggestion(
                    original_value=value,
                    suggested_value=f"{numeric} cGy",
                    unit="cGy",
                    confidence=ConfidenceLevel.LOW,
                    reasoning="Radiation context, larger value suggests cGy",
                    context_used="radiation field",
                )
        
        return None
    
    def _has_units(self, value: str) -> bool:
        """Check if value already has units."""
        for pattern in self.HAS_UNITS_PATTERNS:
            if re.search(pattern, value, re.IGNORECASE):
                return True
        return False


def infer_time_units(
    value: str,
    field_name: str = "",
    study_description: str = "",
) -> Optional[UnitSuggestion]:
    """Convenience function to infer time units."""
    inferrer = UnitInferrer()
    return inferrer.infer_time_units(value, field_name, study_description)


def infer_age_units(
    value: str,
    field_name: str = "",
    study_description: str = "",
    organism: str = "",
) -> Optional[UnitSuggestion]:
    """Convenience function to infer age units."""
    inferrer = UnitInferrer()
    return inferrer.infer_age_units(value, field_name, study_description, organism)

