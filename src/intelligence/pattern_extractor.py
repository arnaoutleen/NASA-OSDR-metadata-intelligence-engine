"""
NASA OSDR Metadata Intelligence Engine - Pattern Extractor

This module extracts structured information from sample names
using pattern matching and heuristics.

Examples:
- "Mmus_C57-6J_BRN_HLU_IR_7d_Rep1_M2" ->
  organism=Mmus, strain=C57-6J, tissue=BRN, condition=HLU+IR,
  timepoint=7d, replicate=1, mouse=2

All extractions are returned as SUGGESTIONS with confidence levels,
not applied directly to metadata.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
import re

from src.core.constants import (
    ORGANISM_SAMPLE_PREFIXES,
    MOUSE_STRAIN_PATTERNS,
    SPACEFLIGHT_INDICATORS,
    GROUND_CONTROL_INDICATORS,
    HLU_INDICATORS,
    RADIATION_INDICATORS,
    ConfidenceLevel,
)


@dataclass
class PatternMatch:
    """A single pattern match from a sample name."""
    field_name: str
    value: str
    start_pos: int
    end_pos: int
    pattern_name: str
    confidence: ConfidenceLevel
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "field_name": self.field_name,
            "value": self.value,
            "pattern_name": self.pattern_name,
            "confidence": self.confidence.value,
        }


@dataclass
class SampleNamePattern:
    """Complete pattern analysis of a sample name."""
    sample_id: str
    matches: List[PatternMatch] = field(default_factory=list)
    unmatched_segments: List[str] = field(default_factory=list)
    
    def get_field(self, field_name: str) -> Optional[PatternMatch]:
        """Get match for a specific field."""
        for match in self.matches:
            if match.field_name == field_name:
                return match
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sample_id": self.sample_id,
            "matches": [m.to_dict() for m in self.matches],
            "unmatched_segments": self.unmatched_segments,
        }


class PatternExtractor:
    """
    Extracts structured information from sample names.
    
    This extractor uses a combination of:
    - Known prefix patterns (organism, strain codes)
    - Positional patterns (timepoints, replicates)
    - Contextual patterns (conditions, treatments)
    """
    
    # Organism prefix patterns
    ORGANISM_PATTERNS = [
        (r'^(Mmus)', 'organism', 'Mus musculus'),
        (r'^(Rnor)', 'organism', 'Rattus norvegicus'),
        (r'^(Hsap)', 'organism', 'Homo sapiens'),
    ]
    
    # Strain patterns (usually after organism)
    STRAIN_PATTERNS = [
        (r'_C57[-_]?6J?', 'strain', 'C57BL/6J'),
        (r'_C57BL', 'strain', 'C57BL/6'),
        (r'_BALB', 'strain', 'BALB/c'),
        (r'_C57[-_]?6', 'strain', 'C57BL/6'),
    ]
    
    # Tissue/organ abbreviations
    TISSUE_PATTERNS = [
        (r'_BRN_', 'tissue', 'Brain'),
        (r'_HRT_', 'tissue', 'Heart'),
        (r'_LVR_', 'tissue', 'Liver'),
        (r'_KDN_', 'tissue', 'Kidney'),
        (r'_SPL_', 'tissue', 'Spleen'),
        (r'_LNG_', 'tissue', 'Lung'),
        (r'_MUS_', 'tissue', 'Muscle'),
        (r'_SKL_', 'tissue', 'Skeletal Muscle'),
        (r'_SOL_', 'tissue', 'Soleus'),
        (r'_GAS_', 'tissue', 'Gastrocnemius'),
        (r'_TA_', 'tissue', 'Tibialis Anterior'),
        (r'_QDR_', 'tissue', 'Quadriceps'),
        (r'_BM_', 'tissue', 'Bone Marrow'),
        (r'_FEM_', 'tissue', 'Femur'),
        (r'_TIB_', 'tissue', 'Tibia'),
        (r'_EYE_', 'tissue', 'Eye'),
        (r'_RET_', 'tissue', 'Retina'),
        (r'_ADR_', 'tissue', 'Adrenal'),
        (r'_THY_', 'tissue', 'Thymus'),
        (r'_PAN_', 'tissue', 'Pancreas'),
        (r'_COL_', 'tissue', 'Colon'),
    ]
    
    # Condition patterns
    CONDITION_PATTERNS = [
        (r'_FLT_', 'condition', 'Flight'),
        (r'_GC_', 'condition', 'Ground Control'),
        (r'_HLU_', 'condition', 'Hindlimb Unloading'),
        (r'_HLLC_', 'condition', 'Normally Loaded Control'),
        (r'_IR_', 'condition', 'Irradiated'),
        (r'_IRC_', 'condition', 'Irradiated Control'),
        (r'_SH_', 'condition', 'Sham'),
        (r'_TT_', 'condition', 'Treatment'),
        (r'_Ctrl_', 'condition', 'Control'),
        (r'_VIV_', 'condition', 'Vivarium Control'),
        (r'_BAS_', 'condition', 'Basal Control'),
    ]
    
    # Timepoint patterns
    TIMEPOINT_PATTERNS = [
        (r'_(\d+)d_', 'timepoint', '{0} days'),
        (r'_(\d+)day_', 'timepoint', '{0} days'),
        (r'_(\d+)h_', 'timepoint', '{0} hours'),
        (r'_(\d+)hr_', 'timepoint', '{0} hours'),
        (r'_(\d+)wk_', 'timepoint', '{0} weeks'),
        (r'_(\d+)week_', 'timepoint', '{0} weeks'),
        (r'_(\d+\.?\d*)mon_', 'timepoint', '{0} months'),
        (r'_(\d+\.?\d*)month_', 'timepoint', '{0} months'),
        (r'_R(\d+)_', 'timepoint', '{0} days post-return'),
        (r'_L(\d+)_', 'timepoint', '{0} days pre-launch'),
    ]
    
    # Replicate patterns
    REPLICATE_PATTERNS = [
        (r'_Rep(\d+)', 'replicate', '{0}'),
        (r'_rep(\d+)', 'replicate', '{0}'),
        (r'_R(\d+)$', 'replicate', '{0}'),
        (r'_B(\d+)', 'bio_replicate', '{0}'),
        (r'_T(\d+)', 'tech_replicate', '{0}'),
    ]
    
    # Mouse/subject ID patterns
    MOUSE_PATTERNS = [
        (r'_M(\d+)(?:_|$)', 'mouse_id', '{0}'),
        (r'_Mouse(\d+)', 'mouse_id', '{0}'),
        (r'_Subj(\d+)', 'subject_id', '{0}'),
    ]
    
    def __init__(self):
        """Initialize pattern extractor."""
        self._all_patterns = (
            self.ORGANISM_PATTERNS +
            self.STRAIN_PATTERNS +
            self.TISSUE_PATTERNS +
            self.CONDITION_PATTERNS
        )
    
    def extract(self, sample_id: str) -> SampleNamePattern:
        """
        Extract all pattern matches from a sample name.
        
        Args:
            sample_id: The sample identifier
            
        Returns:
            SampleNamePattern with all detected patterns
        """
        result = SampleNamePattern(sample_id=sample_id)
        remaining = sample_id
        
        # Extract organism
        for pattern, field, value in self.ORGANISM_PATTERNS:
            match = re.search(pattern, sample_id)
            if match:
                result.matches.append(PatternMatch(
                    field_name=field,
                    value=value,
                    start_pos=match.start(),
                    end_pos=match.end(),
                    pattern_name="organism_prefix",
                    confidence=ConfidenceLevel.HIGH,
                ))
                break
        
        # Extract strain
        for pattern, field, value in self.STRAIN_PATTERNS:
            match = re.search(pattern, sample_id, re.IGNORECASE)
            if match:
                result.matches.append(PatternMatch(
                    field_name=field,
                    value=value,
                    start_pos=match.start(),
                    end_pos=match.end(),
                    pattern_name="strain_code",
                    confidence=ConfidenceLevel.HIGH,
                ))
                break
        
        # Extract tissue
        for pattern, field, value in self.TISSUE_PATTERNS:
            match = re.search(pattern, sample_id, re.IGNORECASE)
            if match:
                result.matches.append(PatternMatch(
                    field_name=field,
                    value=value,
                    start_pos=match.start(),
                    end_pos=match.end(),
                    pattern_name="tissue_abbreviation",
                    confidence=ConfidenceLevel.MEDIUM,
                ))
                break
        
        # Extract conditions
        for pattern, field, value in self.CONDITION_PATTERNS:
            match = re.search(pattern, sample_id, re.IGNORECASE)
            if match:
                result.matches.append(PatternMatch(
                    field_name=field,
                    value=value,
                    start_pos=match.start(),
                    end_pos=match.end(),
                    pattern_name="condition_code",
                    confidence=ConfidenceLevel.MEDIUM,
                ))
        
        # Extract timepoint
        for pattern, field, template in self.TIMEPOINT_PATTERNS:
            match = re.search(pattern, sample_id, re.IGNORECASE)
            if match:
                value = template.format(match.group(1))
                result.matches.append(PatternMatch(
                    field_name=field,
                    value=value,
                    start_pos=match.start(),
                    end_pos=match.end(),
                    pattern_name="timepoint_format",
                    confidence=ConfidenceLevel.MEDIUM,
                ))
                break
        
        # Extract replicate
        for pattern, field, template in self.REPLICATE_PATTERNS:
            match = re.search(pattern, sample_id, re.IGNORECASE)
            if match:
                value = template.format(match.group(1))
                result.matches.append(PatternMatch(
                    field_name=field,
                    value=value,
                    start_pos=match.start(),
                    end_pos=match.end(),
                    pattern_name="replicate_number",
                    confidence=ConfidenceLevel.HIGH,
                ))
                break
        
        # Extract mouse/subject ID
        for pattern, field, template in self.MOUSE_PATTERNS:
            match = re.search(pattern, sample_id, re.IGNORECASE)
            if match:
                value = template.format(match.group(1))
                result.matches.append(PatternMatch(
                    field_name=field,
                    value=value,
                    start_pos=match.start(),
                    end_pos=match.end(),
                    pattern_name="subject_id",
                    confidence=ConfidenceLevel.MEDIUM,
                ))
                break
        
        return result
    
    def extract_batch(
        self,
        sample_ids: List[str],
    ) -> Dict[str, SampleNamePattern]:
        """
        Extract patterns from multiple sample IDs.
        
        Args:
            sample_ids: List of sample identifiers
            
        Returns:
            Dict mapping sample_id -> SampleNamePattern
        """
        return {sid: self.extract(sid) for sid in sample_ids}
    
    def summarize_patterns(
        self,
        patterns: List[SampleNamePattern],
    ) -> Dict[str, List[str]]:
        """
        Summarize unique values found for each field.
        
        Args:
            patterns: List of SampleNamePattern results
            
        Returns:
            Dict mapping field_name -> list of unique values
        """
        summary: Dict[str, set] = {}
        
        for pattern in patterns:
            for match in pattern.matches:
                if match.field_name not in summary:
                    summary[match.field_name] = set()
                summary[match.field_name].add(match.value)
        
        return {k: sorted(list(v)) for k, v in summary.items()}


def extract_patterns(sample_id: str) -> SampleNamePattern:
    """
    Convenience function to extract patterns from a sample ID.
    
    Args:
        sample_id: The sample identifier
        
    Returns:
        SampleNamePattern with detected patterns
    """
    extractor = PatternExtractor()
    return extractor.extract(sample_id)

