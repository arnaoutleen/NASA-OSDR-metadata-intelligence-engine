"""
NASA OSDR Metadata Intelligence Engine - Constants and Controlled Vocabularies

This module defines controlled vocabularies and enumerations for ensuring
biological correctness and consistency in metadata enrichment.

All values are based on NASA OSDR conventions and standard biological nomenclature.
"""

from enum import Enum
from typing import FrozenSet, Dict


# =============================================================================
# Provenance and Confidence Enumerations
# =============================================================================

class ProvenanceSource(str, Enum):
    """
    Enumeration of valid provenance sources for enriched metadata.
    
    Every enriched field must be tagged with one of these sources
    to maintain full traceability.
    """
    OSDR_API_METADATA = "from_osdr_metadata"
    ISA_CHARACTERISTICS = "from_isa_characteristics"
    ISA_FACTOR_VALUES = "from_isa_factor_values"
    ISA_STUDY_FILE = "from_isa_study_file"
    ISA_ASSAY_FILE = "from_isa_assay_file"
    STUDY_DESCRIPTION = "from_study_description"
    MISSION_METADATA = "from_mission_metadata"
    SAMPLE_NAME_INFERENCE = "inferred_from_sample_name_structure"
    CROSS_SAMPLE_LINKING = "inferred_from_cross_sample_linking"
    BIOLOGICAL_RULE = "inferred_from_biological_rule"
    AI_SUGGESTION = "ai_suggestion_unverified"
    MANUAL_CURATION = "manual_curation"
    NOT_APPLICABLE = "not_applicable"


class ConfidenceLevel(str, Enum):
    """
    Confidence levels for enriched metadata values.
    
    - HIGH: Direct extraction from structured API/ISA-Tab fields
    - MEDIUM: Pattern-based inference with clear conventions
    - LOW: Grouping-based or uncertain inference
    - SUGGESTION: AI-generated suggestion requiring human review
    - NA: Not applicable (e.g., mouse fields for cell line studies)
    """
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    SUGGESTION = "suggestion"
    NA = "n/a"


# =============================================================================
# Organism Classifications
# =============================================================================

RODENT_ORGANISMS: FrozenSet[str] = frozenset([
    "Mus musculus",
    "Rattus norvegicus",
])

HUMAN_ORGANISMS: FrozenSet[str] = frozenset([
    "Homo sapiens",
])

MODEL_ORGANISMS: FrozenSet[str] = frozenset([
    "Mus musculus",
    "Rattus norvegicus",
    "Homo sapiens",
    "Drosophila melanogaster",
    "Caenorhabditis elegans",
    "Danio rerio",
    "Arabidopsis thaliana",
])


# =============================================================================
# Controlled Strain Vocabularies
# =============================================================================

CONTROLLED_STRAINS: Dict[str, str] = {
    # Mouse strains - canonical forms
    "c57bl/6j": "C57BL/6J",
    "c57bl/6": "C57BL/6",
    "c57bl/6ntac": "C57BL/6NTac",
    "c57bl/6ncrl": "C57BL/6NCrl",
    "c57bl/6n": "C57BL/6N",
    "balb/c": "BALB/c",
    "balb/cj": "BALB/cJ",
    "129s/j": "129S/J",
    "129s1/svimj": "129S1/SvImJ",
    "fvb/n": "FVB/N",
    "fvb/nj": "FVB/NJ",
    "dba/2j": "DBA/2J",
    "cd-1": "CD-1",
    "icr": "ICR",
    "swiss webster": "Swiss Webster",
    "c3h/hej": "C3H/HeJ",
    "akr/j": "AKR/J",
    "cba/j": "CBA/J",
    "sjl/j": "SJL/J",
    "nzb/binj": "NZB/BlNJ",
    "nzw/lacj": "NZW/LacJ",
    "nod/shiltj": "NOD/ShiLtJ",
    "scid": "SCID",
    # Rat strains
    "sprague dawley": "Sprague Dawley",
    "wistar": "Wistar",
    "long evans": "Long Evans",
    "fischer 344": "Fischer 344",
    "lewis": "Lewis",
}

# Strain patterns for detection
MOUSE_STRAIN_PATTERNS: FrozenSet[str] = frozenset([
    "C57BL", "BALB/c", "129S", "FVB", "DBA", "CD-1", "ICR", "Swiss",
    "C3H", "AKR", "CBA", "SJL", "NZB", "NZW", "NOD", "SCID"
])


# =============================================================================
# Controlled Tissue/Organ Vocabularies
# =============================================================================

CONTROLLED_TISSUES: Dict[str, str] = {
    # Brain regions
    "brain": "Brain",
    "cerebral cortex": "Cerebral Cortex",
    "hippocampus": "Hippocampus",
    "cerebellum": "Cerebellum",
    "hypothalamus": "Hypothalamus",
    "brainstem": "Brainstem",
    # Musculoskeletal
    "skeletal muscle": "Skeletal Muscle",
    "soleus": "Soleus",
    "gastrocnemius": "Gastrocnemius",
    "tibialis anterior": "Tibialis Anterior",
    "quadriceps": "Quadriceps",
    "bone": "Bone",
    "femur": "Femur",
    "tibia": "Tibia",
    "bone marrow": "Bone Marrow",
    # Cardiovascular
    "heart": "Heart",
    "left ventricle": "Left Ventricle",
    "right ventricle": "Right Ventricle",
    "aorta": "Aorta",
    "blood": "Blood",
    "plasma": "Plasma",
    "serum": "Serum",
    # Digestive
    "liver": "Liver",
    "small intestine": "Small Intestine",
    "large intestine": "Large Intestine",
    "colon": "Colon",
    "stomach": "Stomach",
    "pancreas": "Pancreas",
    # Respiratory
    "lung": "Lung",
    "trachea": "Trachea",
    # Urinary
    "kidney": "Kidney",
    "bladder": "Bladder",
    # Immune
    "spleen": "Spleen",
    "thymus": "Thymus",
    "lymph node": "Lymph Node",
    # Endocrine
    "adrenal gland": "Adrenal Gland",
    "thyroid": "Thyroid",
    "pituitary": "Pituitary",
    # Reproductive
    "testis": "Testis",
    "ovary": "Ovary",
    "uterus": "Uterus",
    # Integumentary
    "skin": "Skin",
    # Sensory
    "eye": "Eye",
    "retina": "Retina",
    # Other
    "adipose tissue": "Adipose Tissue",
    "white adipose tissue": "White Adipose Tissue",
    "brown adipose tissue": "Brown Adipose Tissue",
}


# =============================================================================
# Controlled Sex Values
# =============================================================================

CONTROLLED_SEX_VALUES: Dict[str, str] = {
    "m": "Male",
    "male": "Male",
    "f": "Female",
    "female": "Female",
    "mixed": "Mixed",
    "both": "Mixed",
    "male, female": "Mixed",
    "female, male": "Mixed",
    "not applicable": "Not Applicable",
    "n/a": "Not Applicable",
    "na": "Not Applicable",
    "unknown": "Unknown",
}


# =============================================================================
# Controlled Assay Types
# =============================================================================

CONTROLLED_ASSAY_TYPES: Dict[str, str] = {
    # Transcriptomics
    "rna sequencing": "RNA-Seq",
    "rna-seq": "RNA-Seq",
    "rnaseq": "RNA-Seq",
    "transcription profiling": "RNA-Seq",
    "transcriptomics": "RNA-Seq",
    "microarray": "Microarray",
    "gene expression array": "Microarray",
    "expression array": "Microarray",
    # Proteomics
    "proteomics": "Proteomics",
    "mass spectrometry": "Mass Spectrometry",
    "lc-ms/ms": "LC-MS/MS",
    "lc-ms": "LC-MS",
    # Metabolomics
    "metabolomics": "Metabolomics",
    "untargeted metabolomics": "Untargeted Metabolomics",
    "targeted metabolomics": "Targeted Metabolomics",
    # Epigenomics
    "methylation profiling": "Methylation Profiling",
    "chip-seq": "ChIP-Seq",
    "atac-seq": "ATAC-Seq",
    # Single-cell
    "single-cell rna-seq": "scRNA-Seq",
    "scrna-seq": "scRNA-Seq",
    "single cell rna sequencing": "scRNA-Seq",
    # Other
    "whole genome sequencing": "WGS",
    "wgs": "WGS",
    "16s rrna sequencing": "16S rRNA",
    "metagenomics": "Metagenomics",
}


# =============================================================================
# Spaceflight and Ground Study Classifications
# =============================================================================

SPACEFLIGHT_INDICATORS: FrozenSet[str] = frozenset([
    "space flight", "spaceflight", "flight", "flt",
    "iss", "space", "orbital", "microgravity",
])

GROUND_CONTROL_INDICATORS: FrozenSet[str] = frozenset([
    "ground control", "ground", "gc", "vivarium",
    "vivarium control", "basal control", "basal",
    "cohort control", "control",
])

HLU_INDICATORS: FrozenSet[str] = frozenset([
    "hindlimb unloading", "hindlimb unloaded", "hlu",
    "unloaded", "tail suspension", "simulated microgravity",
])

RADIATION_INDICATORS: FrozenSet[str] = frozenset([
    "irradiated", "ir", "radiation", "irradiation",
    "gamma", "proton", "hze", "spe",
])


# =============================================================================
# Sample ID Organism Prefixes
# =============================================================================

ORGANISM_SAMPLE_PREFIXES: Dict[str, str] = {
    "Mmus": "Mus musculus",
    "Rnor": "Rattus norvegicus",
    "Hsap": "Homo sapiens",
    "Dmel": "Drosophila melanogaster",
    "Cele": "Caenorhabditis elegans",
    "Drer": "Danio rerio",
    "Atha": "Arabidopsis thaliana",
}


# =============================================================================
# Time-Related Factor Patterns
# =============================================================================

TIME_FACTOR_PATTERNS: FrozenSet[str] = frozenset([
    "return", "post-flight", "post flight", "after landing", "after return",
    "timepoint", "time point", "collection", "duration", "days",
    "sacrifice", "euthanasia", "sample collection", "dissection",
    "time of sample", "sampling time", "harvest", "necropsy",
    "post-treatment", "after treatment", "time after", "day after",
    "hours post", "days post", "weeks post", "months post",
])

NON_TIME_FACTOR_PATTERNS: FrozenSet[str] = frozenset([
    "group", "treatment", "dose", "condition", "genotype", "strain",
])


# =============================================================================
# Mission Duration Patterns
# =============================================================================

MISSION_DURATION_PATTERNS: Dict[str, str] = {
    # Known mission durations (approximate)
    "rodent research-1": "37 days",
    "rr-1": "37 days",
    "rodent research-3": "30 days",
    "rr-3": "30 days",
    "rodent research-5": "30 days",
    "rr-5": "30 days",
    "rodent research-9": "33 days",
    "rr-9": "33 days",
    "bion-m1": "30 days",
    "sts-131": "15 days",
    "sts-135": "13 days",
}

