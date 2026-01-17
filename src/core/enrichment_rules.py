"""
NASA OSDR Metadata Intelligence Engine - Enrichment Rules

This module provides deterministic, rule-based enrichment functions
for filling missing metadata fields using NASA data sources.

Key principles:
1. NEVER overwrite existing values
2. NEVER hallucinate - only fill when evidence exists
3. Track provenance for every enrichment
4. Record conflicts instead of making arbitrary choices
5. All logic must be deterministic and reproducible
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from src.core.constants import (
    CONTROLLED_STRAINS,
    CONTROLLED_TISSUES,
    CONTROLLED_SEX_VALUES,
    CONTROLLED_ASSAY_TYPES,
    RODENT_ORGANISMS,
    MOUSE_STRAIN_PATTERNS,
    ORGANISM_SAMPLE_PREFIXES,
    SPACEFLIGHT_INDICATORS,
    GROUND_CONTROL_INDICATORS,
    HLU_INDICATORS,
    TIME_FACTOR_PATTERNS,
    ProvenanceSource,
    ConfidenceLevel,
)
from src.core.provenance import ProvenanceEntry, ConflictEntry, ProvenanceTracker


@dataclass
class EnrichmentResult:
    """Result of enriching a single row."""
    enriched_row: Dict[str, Any]
    provenance_entries: List[ProvenanceEntry] = field(default_factory=list)
    conflict_entries: List[ConflictEntry] = field(default_factory=list)
    is_rodent: bool = True


# =============================================================================
# Organism Detection
# =============================================================================

def is_rodent_study(metadata: Dict[str, Any]) -> bool:
    """
    Determine if study organism is a rodent.
    
    Uses multiple signals:
    1. Explicit organism field
    2. Sample strain patterns (e.g., C57BL/6J)
    3. Sample ID prefixes (e.g., Mmus_)
    
    Defaults to False (safe mode) if no clear indication.
    
    Args:
        metadata: Study metadata dict
        
    Returns:
        True if rodent study, False otherwise
    """
    if not metadata:
        return False
    
    # Check 1: Explicit organism field
    organism = metadata.get("organism", "")
    if organism:
        if any(r.lower() in organism.lower() for r in RODENT_ORGANISMS):
            return True
    
    # Check 2: Sample strains
    samples = metadata.get("samples", [])
    for sample in samples[:5]:  # Check first 5 samples
        strain = sample.get("strain", "")
        if strain:
            strain_upper = strain.upper()
            for pattern in MOUSE_STRAIN_PATTERNS:
                if pattern.upper() in strain_upper:
                    return True
    
    # Check 3: Sample ID prefixes
    for sample in samples[:5]:
        sample_id = sample.get("id", sample.get("name", ""))
        if sample_id:
            for prefix, org in ORGANISM_SAMPLE_PREFIXES.items():
                if sample_id.startswith(prefix):
                    if any(r.lower() in org.lower() for r in RODENT_ORGANISMS):
                        return True
    
    return False


# =============================================================================
# Value Normalization
# =============================================================================

def normalize_strain(value: str) -> str:
    """Normalize strain value to canonical form."""
    if not value:
        return ""
    
    value_lower = value.lower().strip()
    return CONTROLLED_STRAINS.get(value_lower, value)


def normalize_sex(value: str) -> str:
    """Normalize sex value to canonical form."""
    if not value:
        return ""
    
    value_lower = value.lower().strip()
    return CONTROLLED_SEX_VALUES.get(value_lower, value)


def normalize_tissue(value: str) -> str:
    """Normalize tissue/organ value to canonical form."""
    if not value:
        return ""
    
    value_lower = value.lower().strip()
    return CONTROLLED_TISSUES.get(value_lower, value)


def normalize_assay_type(value: str) -> str:
    """Normalize assay type to canonical form."""
    if not value:
        return ""
    
    value_lower = value.lower().strip()
    return CONTROLLED_ASSAY_TYPES.get(value_lower, value)


def normalize_space_ground(value: str) -> str:
    """Normalize space/ground classification."""
    if not value:
        return ""
    
    value_lower = value.lower().strip()
    
    # Check spaceflight indicators
    for indicator in SPACEFLIGHT_INDICATORS:
        if indicator in value_lower:
            return "space"
    
    # Check ground control indicators
    for indicator in GROUND_CONTROL_INDICATORS:
        if indicator in value_lower:
            return "ground"
    
    # Check HLU indicators
    for indicator in HLU_INDICATORS:
        if indicator in value_lower:
            return "HLU"
    
    return value


def normalize_age(value: str) -> str:
    """
    Normalize age value to include units when possible.
    
    Attempts to parse numeric values and add units based on context.
    Common patterns:
    - "8" with context -> "8 weeks"
    - "8 weeks" -> "8 weeks"
    - "10 days" -> "10 days"
    
    Args:
        value: The age value to normalize
        
    Returns:
        Normalized age string with units if detectable
    """
    if not value:
        return ""
    
    value_str = str(value).strip()
    
    # If already has units, just clean up
    if re.search(r'\d+\s*(weeks?|wks?|days?|months?|mo|years?|yrs?)', value_str, re.IGNORECASE):
        # Standardize unit names
        value_str = re.sub(r'(\d+)\s*wks?\b', r'\1 weeks', value_str, flags=re.IGNORECASE)
        value_str = re.sub(r'(\d+)\s*mo\b', r'\1 months', value_str, flags=re.IGNORECASE)
        value_str = re.sub(r'(\d+)\s*yrs?\b', r'\1 years', value_str, flags=re.IGNORECASE)
        return value_str
    
    # Try to parse as a plain number - don't add units without context
    # (the caller should provide context for unit inference)
    try:
        num = float(value_str)
        # Return as-is if just a number - units need external context
        return value_str
    except ValueError:
        pass
    
    return value_str


# =============================================================================
# Characteristic Extraction
# =============================================================================

def extract_characteristic_from_samples(
    samples: List[Dict[str, Any]],
    category: str,
) -> Optional[str]:
    """
    Extract a characteristic value from samples.
    
    Returns the most common non-empty value.
    
    Args:
        samples: List of sample dicts
        category: The characteristic category to extract
        
    Returns:
        Most common value, or None if not found
    """
    if not samples:
        return None
    
    values: Dict[str, int] = {}
    category_lower = category.lower()
    
    for sample in samples:
        # Check direct fields first
        if category_lower in ["strain", "sex", "age"]:
            direct_value = sample.get(category_lower, "")
            if direct_value:
                values[direct_value] = values.get(direct_value, 0) + 1
        
        # Check material_type for organ/tissue
        if category_lower in ["organism part", "tissue", "organ", "material"]:
            mat_value = sample.get("material_type", "")
            if mat_value:
                values[mat_value] = values.get(mat_value, 0) + 1
        
        # Check characteristics array
        chars = sample.get("characteristics", [])
        if isinstance(chars, list):
            for char in chars:
                cat = str(char.get("category", "")).lower()
                val = str(char.get("value", "")).strip()
                if category_lower in cat and val:
                    values[val] = values.get(val, 0) + 1
        
        # Check factor_values as fallback
        fv = sample.get("factor_values", {})
        if isinstance(fv, dict):
            for key, val in fv.items():
                if val and category_lower in key.lower():
                    val_str = str(val).strip()
                    if val_str and val_str.lower() not in ["not applicable", "na", "n/a"]:
                        values[val_str] = values.get(val_str, 0) + 1
    
    if values:
        return max(values.keys(), key=lambda x: values[x])
    
    return None


def extract_sample_characteristic(
    sample: Dict[str, Any],
    category: str,
) -> Optional[str]:
    """
    Extract a specific characteristic from a single sample.
    
    Args:
        sample: Sample dict
        category: The characteristic category
        
    Returns:
        Value if found, None otherwise
    """
    if not sample:
        return None
    
    category_lower = category.lower()
    
    # Check direct fields
    if category_lower in ["strain", "sex", "age"]:
        value = sample.get(category_lower, "")
        if value:
            return str(value).strip()
    
    # Check material_type
    if category_lower in ["organism part", "tissue", "organ", "material"]:
        value = sample.get("material_type", "")
        if value:
            return str(value).strip()
    
    # Check characteristics array
    chars = sample.get("characteristics", [])
    if isinstance(chars, list):
        for char in chars:
            cat = str(char.get("category", "")).lower()
            val = str(char.get("value", "")).strip()
            if category_lower in cat and val:
                return val
    
    return None


def extract_sample_factor(
    sample: Dict[str, Any],
    factor_name: str,
) -> Optional[str]:
    """
    Extract a specific factor value from a sample.
    
    Args:
        sample: Sample dict
        factor_name: The factor name to look for
        
    Returns:
        Value if found, None otherwise
    """
    if not sample:
        return None
    
    factor_lower = factor_name.lower()
    
    fv = sample.get("factor_values", {})
    
    if isinstance(fv, dict):
        for key, val in fv.items():
            if val and factor_lower in key.lower():
                return str(val).strip()
    
    elif isinstance(fv, list):
        for item in fv:
            cat = str(item.get("category", item.get("name", ""))).lower()
            val = str(item.get("value", "")).strip()
            if val and factor_lower in cat:
                return val
    
    return None


# =============================================================================
# Timepoint Extraction
# =============================================================================

def extract_timepoint_from_sample_id(sample_id: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract timepoint from sample ID naming convention.
    
    Examples:
    - "1_EPM_7.5months" -> ("7.5 months", "sample_id_months")
    - "Brain_7day_Ctrl_2" -> ("7 days", "sample_id_days")
    
    Args:
        sample_id: The sample identifier
        
    Returns:
        Tuple of (timepoint_value, inference_rule) or (None, None)
    """
    if not sample_id:
        return (None, None)
    
    patterns = [
        (r'[_-](\d+\.?\d*)\s*months?(?:[_-]|$)', 'sample_id_months', 'months'),
        (r'[_-](\d+\.?\d*)\s*mon(?:[_-]|$)', 'sample_id_mon', 'months'),
        (r'[_-](\d+\.?\d*)\s*days?(?:[_-]|$)', 'sample_id_days', 'days'),
        (r'[_-](\d+)d(?:[_-]|$)', 'sample_id_d', 'days'),
        (r'[_-](\d+\.?\d*)\s*(?:weeks?|wks?)(?:[_-]|$)', 'sample_id_weeks', 'weeks'),
        (r'[_-](\d+\.?\d*)\s*(?:hours?|hrs?|h)(?:[_-]|$)', 'sample_id_hours', 'hours'),
    ]
    
    for pattern, rule, unit in patterns:
        match = re.search(pattern, sample_id, re.IGNORECASE)
        if match:
            value = match.group(1)
            return (f"{value} {unit}", rule)
    
    return (None, None)


def extract_timepoint_from_factors(
    sample: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract timepoint from factor values.
    
    Looks for time-related factors like "Timepoint", "Time of Sample Collection", etc.
    
    Args:
        sample: Sample dict
        
    Returns:
        Tuple of (timepoint_value, factor_name) or (None, None)
    """
    fv = sample.get("factor_values", {})
    
    time_related_keys = [
        "timepoint", "time point", "time of sample", "time of collection",
        "sample collection time", "dissection time", "harvest time",
        "sacrifice time", "euthanasia time", "post-flight", "after landing",
    ]
    
    if isinstance(fv, dict):
        for key, val in fv.items():
            key_lower = key.lower()
            for time_key in time_related_keys:
                if time_key in key_lower and val:
                    return (str(val).strip(), key)
    
    return (None, None)


# =============================================================================
# Subject Inference
# =============================================================================

def infer_subject_id(
    sample: Dict[str, Any],
    sample_id: str = "",
) -> Tuple[Optional[str], int, Optional[str]]:
    """
    Infer subject/animal ID from sample data with confidence tiers.
    
    Tier 1 (HIGH): Explicit source_name, Subject ID, Animal ID in characteristics
    Tier 2 (MEDIUM): Pattern like _M2, _Mouse2, _B1 in sample name
    Tier 3 (LOW): Grouping by _Rep1/2/3 prefix only
    
    Args:
        sample: Sample dict
        sample_id: Sample identifier (optional)
        
    Returns:
        Tuple of (subject_id, confidence_tier, inference_rule)
    """
    if not sample and not sample_id:
        return (None, 0, None)
    
    sid = sample_id or (sample.get("id", sample.get("name", "")) if sample else "")
    
    # Tier 1: Check explicit fields
    if sample:
        source_name = sample.get("source_name", "")
        if source_name and str(source_name).strip():
            return (str(source_name).strip(), 1, "tier1_source_name")
        
        chars = sample.get("characteristics", [])
        if isinstance(chars, list):
            for char in chars:
                cat = str(char.get("category", "")).lower()
                val = str(char.get("value", "")).strip()
                if val and any(k in cat for k in ["subject", "animal id", "mouse id", "specimen"]):
                    return (val, 1, "tier1_characteristics")
    
    # Tier 2: Parse sample name for mouse ID patterns
    if sid:
        patterns = [
            (r'_M(\d+)(?:_|$)', 'tier2_m_pattern'),
            (r'_Mouse(\d+)(?:_|$)', 'tier2_mouse_pattern'),
            (r'_B(\d+)(?:_|$)', 'tier2_b_pattern'),
            (r'_([A-Z]\d+)$', 'tier2_letter_number'),
        ]
        for pattern, rule in patterns:
            match = re.search(pattern, sid, re.IGNORECASE)
            if match:
                return (match.group(1), 2, rule)
    
    # Tier 3: Group by prefix before _Rep{N}
    if sid:
        match = re.match(r'^(.+?)_Rep\d+', sid, re.IGNORECASE)
        if match:
            prefix = match.group(1)
            return (prefix, 3, "tier3_rep_prefix")
    
    return (None, 0, None)


# =============================================================================
# Main Enrichment Function
# =============================================================================

def enrich_row(
    row: Dict[str, Any],
    osdr_study_json: Dict[str, Any],
    isa_metadata: Optional[Dict[str, Any]] = None,
    tracker: Optional[ProvenanceTracker] = None,
) -> EnrichmentResult:
    """
    Enrich a single CSV row using OSDR metadata.
    
    This is the main entry point for row-level enrichment.
    Only fills empty cells - never overwrites existing values.
    
    Args:
        row: The CSV row as a dict (field_name -> value)
        osdr_study_json: Complete study metadata from OSDR API
        isa_metadata: Optional ISA-Tab parsed metadata
        tracker: Optional provenance tracker for logging
        
    Returns:
        EnrichmentResult with enriched row, provenance, and conflicts
    """
    result = EnrichmentResult(
        enriched_row=row.copy(),
        provenance_entries=[],
        conflict_entries=[],
    )
    
    if not osdr_study_json:
        return result
    
    # Get OSD ID and sample ID
    osd_id = _extract_osd_id(row, osdr_study_json)
    sample_id = _extract_sample_id(row)
    
    # Check if rodent study
    result.is_rodent = is_rodent_study(osdr_study_json)
    
    # Get samples from metadata
    samples = osdr_study_json.get("samples", [])
    
    # Find matching sample for sample-level enrichment
    matching_sample = _find_matching_sample(sample_id, samples)
    
    # Merge with ISA-Tab if available
    if isa_metadata and not matching_sample:
        isa_samples = isa_metadata.get("samples", [])
        matching_sample = _find_matching_sample(sample_id, isa_samples)
    
    # Apply enrichment rules
    _enrich_mouse_strain(result, osd_id, sample_id, samples, matching_sample, tracker)
    _enrich_mouse_sex(result, osd_id, sample_id, samples, matching_sample, tracker)
    _enrich_age(result, osd_id, sample_id, samples, matching_sample, tracker)
    _enrich_mouse_id(result, osd_id, sample_id, matching_sample, tracker)
    _enrich_organ_sampled(result, osd_id, sample_id, samples, matching_sample, osdr_study_json, tracker)
    _enrich_space_or_ground(result, osd_id, sample_id, matching_sample, tracker)
    _enrich_timepoint(result, osd_id, sample_id, matching_sample, tracker)
    _enrich_assay_type(result, osd_id, osdr_study_json, tracker)
    
    # New study-level enrichments
    _enrich_n_mice_total(result, osd_id, samples, osdr_study_json, tracker)
    _enrich_has_rnaseq(result, osd_id, osdr_study_json, tracker)
    _enrich_genetic_variant(result, osd_id, samples, tracker)
    _enrich_mouse_source(result, osd_id, samples, tracker)
    _enrich_time_in_space(result, osd_id, samples, osdr_study_json, tracker)
    _enrich_study_purpose(result, osd_id, osdr_study_json, tracker)
    _enrich_n_rnaseq_mice(result, osd_id, samples, osdr_study_json, tracker)
    _enrich_age_when_sent_to_space(result, osd_id, samples, tracker)
    
    return result


# =============================================================================
# Helper Functions
# =============================================================================

def _should_enrich(row: Dict[str, Any], field_name: str) -> bool:
    """Check if a field should be enriched (is empty)."""
    value = row.get(field_name, "")
    return not value or not str(value).strip()


def _extract_osd_id(row: Dict[str, Any], metadata: Dict[str, Any]) -> str:
    """Extract OSD ID from row or metadata."""
    # Try canonical name first (from flexible loader), then fallbacks
    for key in ["osd_id", "OSD_study", "osd_study", "OSD", "study_id", "accession"]:
        if key in row and row[key]:
            return str(row[key]).strip()
    
    # Fall back to metadata
    return metadata.get("accession", "")


def _extract_sample_id(row: Dict[str, Any]) -> str:
    """Extract sample ID from row."""
    # Try canonical name first (from flexible loader), then fallbacks
    for key in ["sample_id", "mouse_uid", "Sample Name", "sample_name", "source_name"]:
        if key in row and row[key]:
            return str(row[key]).strip()
    return ""


def _find_matching_sample(
    sample_id: str,
    samples: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Find sample in list by ID."""
    if not sample_id or not samples:
        return None
    
    for sample in samples:
        s_id = sample.get("id", sample.get("name", sample.get("sample_name", "")))
        if s_id == sample_id or sample_id in s_id or s_id in sample_id:
            return sample
    
    return None


def _enrich_mouse_strain(
    result: EnrichmentResult,
    osd_id: str,
    sample_id: str,
    samples: List[Dict[str, Any]],
    matching_sample: Optional[Dict[str, Any]],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich mouse_strain field."""
    field_name = "mouse_strain"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    if not result.is_rodent:
        # Non-rodent study - mark as N/A
        result.enriched_row[field_name] = "N/A - non-rodent study"
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id=sample_id or "_study_level_",
            field_name=field_name,
            value="N/A - non-rodent study",
            source=ProvenanceSource.NOT_APPLICABLE,
            confidence=ConfidenceLevel.NA,
        )
        result.provenance_entries.append(entry)
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value="N/A - non-rodent study",
                source=ProvenanceSource.NOT_APPLICABLE,
                confidence=ConfidenceLevel.NA,
                sample_id=sample_id,
            )
        return
    
    # Try sample-specific first
    value = None
    source = ProvenanceSource.OSDR_API_METADATA
    
    if matching_sample:
        value = extract_sample_characteristic(matching_sample, "strain")
        source = ProvenanceSource.ISA_CHARACTERISTICS
    
    # Fall back to study-level
    if not value:
        value = extract_characteristic_from_samples(samples, "strain")
        source = ProvenanceSource.OSDR_API_METADATA
    
    if value:
        normalized = normalize_strain(value)
        result.enriched_row[field_name] = normalized
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id=sample_id or "_study_level_",
            field_name=field_name,
            value=normalized,
            source=source,
            confidence=ConfidenceLevel.HIGH,
            original_value=value if value != normalized else None,
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=normalized,
                source=source,
                confidence=ConfidenceLevel.HIGH,
                sample_id=sample_id,
                original_value=value if value != normalized else None,
            )


def _enrich_mouse_sex(
    result: EnrichmentResult,
    osd_id: str,
    sample_id: str,
    samples: List[Dict[str, Any]],
    matching_sample: Optional[Dict[str, Any]],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich mouse_sex field."""
    field_name = "mouse_sex"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    if not result.is_rodent:
        result.enriched_row[field_name] = "N/A"
        return
    
    value = None
    source = ProvenanceSource.OSDR_API_METADATA
    
    if matching_sample:
        value = extract_sample_characteristic(matching_sample, "sex")
        source = ProvenanceSource.ISA_CHARACTERISTICS
    
    if not value:
        value = extract_characteristic_from_samples(samples, "sex")
    
    if value:
        normalized = normalize_sex(value)
        result.enriched_row[field_name] = normalized
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id=sample_id or "_study_level_",
            field_name=field_name,
            value=normalized,
            source=source,
            confidence=ConfidenceLevel.HIGH,
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=normalized,
                source=source,
                confidence=ConfidenceLevel.HIGH,
                sample_id=sample_id,
            )


def _enrich_organ_sampled(
    result: EnrichmentResult,
    osd_id: str,
    sample_id: str,
    samples: List[Dict[str, Any]],
    matching_sample: Optional[Dict[str, Any]],
    metadata: Dict[str, Any],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich organ_sampled field."""
    field_name = "organ_sampled"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    value = None
    source = ProvenanceSource.OSDR_API_METADATA
    
    if matching_sample:
        value = extract_sample_characteristic(matching_sample, "organism part")
        source = ProvenanceSource.ISA_CHARACTERISTICS
    
    if not value:
        value = extract_characteristic_from_samples(samples, "organism part")
    
    if not value:
        value = metadata.get("material_type", "")
    
    if value:
        normalized = normalize_tissue(value)
        result.enriched_row[field_name] = normalized
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id=sample_id or "_study_level_",
            field_name=field_name,
            value=normalized,
            source=source,
            confidence=ConfidenceLevel.HIGH,
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=normalized,
                source=source,
                confidence=ConfidenceLevel.HIGH,
                sample_id=sample_id,
            )


def _enrich_space_or_ground(
    result: EnrichmentResult,
    osd_id: str,
    sample_id: str,
    matching_sample: Optional[Dict[str, Any]],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich space_or_ground field."""
    field_name = "space_or_ground"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    if not matching_sample:
        return
    
    value = extract_sample_factor(matching_sample, "spaceflight")
    if not value:
        value = extract_sample_factor(matching_sample, "group")
    
    if value:
        normalized = normalize_space_ground(value)
        result.enriched_row[field_name] = normalized
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id=sample_id,
            field_name=field_name,
            value=normalized,
            source=ProvenanceSource.ISA_FACTOR_VALUES,
            confidence=ConfidenceLevel.HIGH,
            original_value=value if value != normalized else None,
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=normalized,
                source=ProvenanceSource.ISA_FACTOR_VALUES,
                confidence=ConfidenceLevel.HIGH,
                sample_id=sample_id,
            )


def _enrich_timepoint(
    result: EnrichmentResult,
    osd_id: str,
    sample_id: str,
    matching_sample: Optional[Dict[str, Any]],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich when_was_the_sample_collected field."""
    field_name = "when_was_the_sample_collected"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    value = None
    source = ProvenanceSource.ISA_FACTOR_VALUES
    confidence = ConfidenceLevel.HIGH
    inference_rule = None
    
    # Try factor values first
    if matching_sample:
        value, factor_name = extract_timepoint_from_factors(matching_sample)
        if value:
            inference_rule = f"factor_{factor_name}"
    
    # Fall back to sample ID parsing
    if not value and sample_id:
        value, rule = extract_timepoint_from_sample_id(sample_id)
        if value:
            source = ProvenanceSource.SAMPLE_NAME_INFERENCE
            confidence = ConfidenceLevel.MEDIUM
            inference_rule = rule
    
    if value:
        result.enriched_row[field_name] = value
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id=sample_id or "_study_level_",
            field_name=field_name,
            value=value,
            source=source,
            confidence=confidence,
            inference_rule=inference_rule,
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=value,
                source=source,
                confidence=confidence,
                sample_id=sample_id,
                inference_rule=inference_rule,
            )


def _enrich_mouse_id(
    result: EnrichmentResult,
    osd_id: str,
    sample_id: str,
    matching_sample: Optional[Dict[str, Any]],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """
    Enrich mouse_id field using subject inference.
    
    Uses the infer_subject_id() function which has 3 confidence tiers:
    - Tier 1 (HIGH): Explicit source_name or Subject ID in characteristics
    - Tier 2 (MEDIUM): Pattern like _M2, _Mouse2 in sample name
    - Tier 3 (LOW): Grouping by _Rep prefix only
    
    Args:
        result: The enrichment result to update
        osd_id: The OSD study identifier
        sample_id: The sample identifier
        matching_sample: The specific sample matching sample_id
        tracker: Optional provenance tracker
    """
    field_name = "mouse_id"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    # Use the existing infer_subject_id function
    subject_id, tier, inference_rule = infer_subject_id(matching_sample, sample_id)
    
    if not subject_id or tier == 0:
        return
    
    # Map tier to confidence level
    confidence_map = {
        1: ConfidenceLevel.HIGH,
        2: ConfidenceLevel.MEDIUM,
        3: ConfidenceLevel.LOW,
    }
    confidence = confidence_map.get(tier, ConfidenceLevel.LOW)
    
    # Map tier to provenance source
    source_map = {
        1: ProvenanceSource.ISA_CHARACTERISTICS,  # tier1 comes from explicit fields
        2: ProvenanceSource.SAMPLE_NAME_INFERENCE,  # tier2 comes from sample name patterns
        3: ProvenanceSource.SAMPLE_NAME_INFERENCE,  # tier3 comes from replicate grouping
    }
    source = source_map.get(tier, ProvenanceSource.SAMPLE_NAME_INFERENCE)
    
    result.enriched_row[field_name] = subject_id
    
    entry = ProvenanceEntry(
        osd_id=osd_id,
        sample_id=sample_id or "_study_level_",
        field_name=field_name,
        value=subject_id,
        source=source,
        confidence=confidence,
        inference_rule=inference_rule,
    )
    result.provenance_entries.append(entry)
    
    if tracker:
        tracker.record(
            osd_id=osd_id,
            field_name=field_name,
            value=subject_id,
            source=source,
            confidence=confidence,
            sample_id=sample_id,
            inference_rule=inference_rule,
        )


def _enrich_age(
    result: EnrichmentResult,
    osd_id: str,
    sample_id: str,
    samples: List[Dict[str, Any]],
    matching_sample: Optional[Dict[str, Any]],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """
    Enrich age field from sample characteristics or factors.
    
    Priority:
    1. Sample-specific characteristic (age category)
    2. Study-level age from samples
    3. Factor values containing age info
    
    Args:
        result: The enrichment result to update
        osd_id: The OSD study identifier
        sample_id: The sample identifier
        samples: List of all samples in the study
        matching_sample: The specific sample matching sample_id
        tracker: Optional provenance tracker
    """
    field_name = "age"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    value = None
    source = ProvenanceSource.OSDR_API_METADATA
    confidence = ConfidenceLevel.HIGH
    original_value = None
    
    # Priority 1: Try sample-specific characteristic
    if matching_sample:
        value = extract_sample_characteristic(matching_sample, "age")
        if value:
            source = ProvenanceSource.ISA_CHARACTERISTICS
    
    # Priority 2: Try study-level extraction from samples
    if not value:
        value = extract_characteristic_from_samples(samples, "age")
        if value:
            source = ProvenanceSource.OSDR_API_METADATA
    
    # Priority 3: Try factor values (some studies encode age as a factor)
    if not value and matching_sample:
        fv = matching_sample.get("factor_values", {})
        if isinstance(fv, dict):
            for key, val in fv.items():
                key_lower = key.lower()
                if "age" in key_lower and val:
                    value = str(val).strip()
                    source = ProvenanceSource.ISA_FACTOR_VALUES
                    break
    
    if value:
        original_value = value
        normalized = normalize_age(value)
        result.enriched_row[field_name] = normalized
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id=sample_id or "_study_level_",
            field_name=field_name,
            value=normalized,
            source=source,
            confidence=confidence,
            original_value=original_value if original_value != normalized else None,
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=normalized,
                source=source,
                confidence=confidence,
                sample_id=sample_id,
                original_value=original_value if original_value != normalized else None,
            )


def _enrich_assay_type(
    result: EnrichmentResult,
    osd_id: str,
    metadata: Dict[str, Any],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich assay_on_organ field."""
    field_name = "assay_on_organ"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    assay_types = metadata.get("assay_types", "")
    if not assay_types:
        assays = metadata.get("assays", [])
        if assays:
            types = set()
            for assay in assays:
                atype = assay.get("type", "")
                if atype:
                    types.add(normalize_assay_type(atype))
            assay_types = ", ".join(sorted(types))
    else:
        # Clean up multi-space delimiter
        types = [normalize_assay_type(t.strip()) for t in assay_types.split("     ") if t.strip()]
        assay_types = ", ".join(types)
    
    if assay_types:
        result.enriched_row[field_name] = assay_types
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id="_study_level_",
            field_name=field_name,
            value=assay_types,
            source=ProvenanceSource.OSDR_API_METADATA,
            confidence=ConfidenceLevel.HIGH,
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=assay_types,
                source=ProvenanceSource.OSDR_API_METADATA,
                confidence=ConfidenceLevel.HIGH,
            )


def _enrich_n_mice_total(
    result: EnrichmentResult,
    osd_id: str,
    samples: List[Dict[str, Any]],
    metadata: Dict[str, Any],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich n_mice_total field by counting unique source_names (animals)."""
    field_name = "n_mice_total"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    if not samples:
        return
    
    # Count unique source_names (each represents one animal)
    source_names = set()
    for sample in samples:
        sn = sample.get("source_name", "")
        if sn:
            source_names.add(sn)
    
    if source_names:
        value = str(len(source_names))
        result.enriched_row[field_name] = value
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id="_study_level_",
            field_name=field_name,
            value=value,
            source=ProvenanceSource.OSDR_API_METADATA,
            confidence=ConfidenceLevel.HIGH,
            evidence_path="samples[].source_name (unique count)",
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=value,
                source=ProvenanceSource.OSDR_API_METADATA,
                confidence=ConfidenceLevel.HIGH,
                evidence_path="samples[].source_name (unique count)",
            )


def _enrich_has_rnaseq(
    result: EnrichmentResult,
    osd_id: str,
    metadata: Dict[str, Any],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich Has_RNAseq field by checking assay types."""
    field_name = "Has_RNAseq"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    # Check assays for RNA-seq
    has_rnaseq = "No"
    assays = metadata.get("assays", [])
    for assay in assays:
        filename = assay.get("filename", "")
        if "rna-seq" in filename.lower() or "rnaseq" in filename.lower() or "rna seq" in filename.lower():
            has_rnaseq = "Yes"
            break
    
    # Also check assay_types field
    if has_rnaseq == "No":
        assay_types = metadata.get("assay_types", "").lower()
        if "rna-seq" in assay_types or "rnaseq" in assay_types:
            has_rnaseq = "Yes"
    
    result.enriched_row[field_name] = has_rnaseq
    
    entry = ProvenanceEntry(
        osd_id=osd_id,
        sample_id="_study_level_",
        field_name=field_name,
        value=has_rnaseq,
        source=ProvenanceSource.OSDR_API_METADATA,
        confidence=ConfidenceLevel.HIGH,
        evidence_path="assays[].filename",
    )
    result.provenance_entries.append(entry)
    
    if tracker:
        tracker.record(
            osd_id=osd_id,
            field_name=field_name,
            value=has_rnaseq,
            source=ProvenanceSource.OSDR_API_METADATA,
            confidence=ConfidenceLevel.HIGH,
        )


def _enrich_genetic_variant(
    result: EnrichmentResult,
    osd_id: str,
    samples: List[Dict[str, Any]],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich mouse_genetic_variant field from Genotype characteristic."""
    field_name = "mouse_genetic_variant"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    genotype = None
    for sample in samples[:10]:  # Check first 10 samples
        chars = sample.get("characteristics", [])
        for char in chars:
            cat = str(char.get("category", "")).lower()
            val = str(char.get("value", "")).strip()
            if "genotype" in cat and val and val.lower() not in ["n/a", "na", ""]:
                genotype = val
                break
        if genotype:
            break
    
    if genotype:
        result.enriched_row[field_name] = genotype
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id="_study_level_",
            field_name=field_name,
            value=genotype,
            source=ProvenanceSource.ISA_CHARACTERISTICS,
            confidence=ConfidenceLevel.HIGH,
            evidence_path="characteristics[].Genotype",
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=genotype,
                source=ProvenanceSource.ISA_CHARACTERISTICS,
                confidence=ConfidenceLevel.HIGH,
            )


def _enrich_mouse_source(
    result: EnrichmentResult,
    osd_id: str,
    samples: List[Dict[str, Any]],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich mouse_source field from Animal Source characteristic."""
    field_name = "mouse_source"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    source = None
    for sample in samples[:10]:  # Check first 10 samples
        chars = sample.get("characteristics", [])
        for char in chars:
            cat = str(char.get("category", "")).lower()
            val = str(char.get("value", "")).strip()
            if "animal source" in cat and val and val.lower() not in ["n/a", "na", ""]:
                source = val
                break
        if source:
            break
    
    if source:
        result.enriched_row[field_name] = source
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id="_study_level_",
            field_name=field_name,
            value=source,
            source=ProvenanceSource.ISA_CHARACTERISTICS,
            confidence=ConfidenceLevel.HIGH,
            evidence_path="characteristics[].Animal Source",
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=source,
                source=ProvenanceSource.ISA_CHARACTERISTICS,
                confidence=ConfidenceLevel.HIGH,
            )


def _enrich_time_in_space(
    result: EnrichmentResult,
    osd_id: str,
    samples: List[Dict[str, Any]],
    metadata: Dict[str, Any],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich time_in_space field from Duration factor or description."""
    field_name = "time_in_space"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    duration = None
    
    # Check factor values for Duration
    for sample in samples[:10]:
        fv = sample.get("factor_values", {})
        if isinstance(fv, dict):
            for key, val in fv.items():
                if "duration" in key.lower() and val:
                    duration = str(val).strip()
                    break
        if duration:
            break
    
    # Try to extract from description if not found
    if not duration:
        description = metadata.get("description", "")
        # Look for patterns like "33 days", "35 days in space"
        match = re.search(r'(\d+)\s*days?\s*(?:in\s+space|on\s+the?\s+ISS|mission)', description, re.IGNORECASE)
        if match:
            duration = f"{match.group(1)} days"
    
    if duration:
        result.enriched_row[field_name] = duration
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id="_study_level_",
            field_name=field_name,
            value=duration,
            source=ProvenanceSource.OSDR_API_METADATA,
            confidence=ConfidenceLevel.MEDIUM if "description" in str(duration) else ConfidenceLevel.HIGH,
            evidence_path="factor_values[].Duration or description",
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=duration,
                source=ProvenanceSource.OSDR_API_METADATA,
                confidence=ConfidenceLevel.MEDIUM,
            )


def _enrich_study_purpose(
    result: EnrichmentResult,
    osd_id: str,
    metadata: Dict[str, Any],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich study_purpose field from description."""
    field_name = "study purpose"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    description = metadata.get("description", "")
    if description:
        # Truncate to first 200 chars for summary
        if len(description) > 200:
            # Find a good break point
            value = description[:200].rsplit(" ", 1)[0] + "..."
        else:
            value = description
        
        result.enriched_row[field_name] = value
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id="_study_level_",
            field_name=field_name,
            value=value,
            source=ProvenanceSource.OSDR_API_METADATA,
            confidence=ConfidenceLevel.HIGH,
            evidence_path="description",
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=value,
                source=ProvenanceSource.OSDR_API_METADATA,
                confidence=ConfidenceLevel.HIGH,
            )


def _enrich_n_rnaseq_mice(
    result: EnrichmentResult,
    osd_id: str,
    samples: List[Dict[str, Any]],
    metadata: Dict[str, Any],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich n_RNAseq_mice field - count of samples with RNA-seq data."""
    field_name = "n_RNAseq_mice"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    # Check if study has RNA-seq
    has_rnaseq = False
    assays = metadata.get("assays", [])
    for assay in assays:
        filename = assay.get("filename", "")
        if "rna-seq" in filename.lower() or "rnaseq" in filename.lower():
            has_rnaseq = True
            break
    
    if has_rnaseq:
        # Count unique source_names (animals)
        source_names = set()
        for sample in samples:
            sn = sample.get("source_name", "")
            if sn:
                source_names.add(sn)
        
        value = str(len(source_names))
        result.enriched_row[field_name] = value
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id="_study_level_",
            field_name=field_name,
            value=value,
            source=ProvenanceSource.OSDR_API_METADATA,
            confidence=ConfidenceLevel.MEDIUM,
            evidence_path="samples with RNA-seq assay",
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=value,
                source=ProvenanceSource.OSDR_API_METADATA,
                confidence=ConfidenceLevel.MEDIUM,
            )


def _enrich_age_when_sent_to_space(
    result: EnrichmentResult,
    osd_id: str,
    samples: List[Dict[str, Any]],
    tracker: Optional[ProvenanceTracker],
) -> None:
    """Enrich age_when_sent_to_space field from Age at Launch characteristic."""
    field_name = "age_when_sent_to_space"
    
    if not _should_enrich(result.enriched_row, field_name):
        return
    
    age_at_launch = None
    for sample in samples[:10]:
        chars = sample.get("characteristics", [])
        for char in chars:
            cat = str(char.get("category", "")).lower()
            val = str(char.get("value", "")).strip()
            if "age at launch" in cat and val:
                age_at_launch = val
                break
            # Also check for just "age" if no launch-specific found
            if cat == "age" and val and not age_at_launch:
                age_at_launch = val
        if age_at_launch and "launch" in cat:
            break
    
    if age_at_launch:
        result.enriched_row[field_name] = age_at_launch
        
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id="_study_level_",
            field_name=field_name,
            value=age_at_launch,
            source=ProvenanceSource.ISA_CHARACTERISTICS,
            confidence=ConfidenceLevel.HIGH,
            evidence_path="characteristics[].Age at Launch",
        )
        result.provenance_entries.append(entry)
        
        if tracker:
            tracker.record(
                osd_id=osd_id,
                field_name=field_name,
                value=age_at_launch,
                source=ProvenanceSource.ISA_CHARACTERISTICS,
                confidence=ConfidenceLevel.HIGH,
            )

