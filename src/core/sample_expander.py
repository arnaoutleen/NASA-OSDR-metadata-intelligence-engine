"""
NASA OSDR Metadata Intelligence Engine - Sample Expander

This module provides functionality to expand OSD IDs into sample-level rows
by parsing ISA-Tab Study (s_*.txt) and Assay (a_*.txt) files.

The expander follows the column mapping rules from the bioinformatics intern's
workflow documentation to generate properly formatted sample summaries.
"""

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.core.osdr_client import OSDRClient
from src.core.isa_parser import ISAParser


@dataclass
class SampleRow:
    """A single output row representing one sample."""
    RR_mission: str = ""
    OSD_study: str = ""
    mouse_uid: str = ""
    sample_name: str = ""
    extract_name: str = ""
    space_or_ground: str = ""
    when_was_the_sample_collected: str = ""
    mouse_sex: str = ""
    mouse_strain: str = ""
    mouse_genetic_variant: str = ""
    mouse_source: str = ""
    organ_sampled: str = ""
    assay_on_organ: str = ""
    number_of_tech_replicates: str = ""
    part_of_a_longitudinal_sample_series: str = ""
    notes: str = ""
    RNA_seq_method: str = ""
    RNA_seq_paired: str = ""
    days_in_space_rr3: str = ""
    
    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for CSV output."""
        return {
            "RR_mission": self.RR_mission,
            "OSD_study": self.OSD_study,
            "mouse_uid": self.mouse_uid,
            "sample_name": self.sample_name,
            "extract_name": self.extract_name,
            "space_or_ground": self.space_or_ground,
            "when_was_the_sample_collected": self.when_was_the_sample_collected,
            "mouse_sex": self.mouse_sex,
            "mouse_strain": self.mouse_strain,
            "mouse_genetic_variant": self.mouse_genetic_variant,
            "mouse_source": self.mouse_source,
            "organ_sampled": self.organ_sampled,
            "assay_on_organ": self.assay_on_organ,
            "number_of_tech_replicates": self.number_of_tech_replicates,
            "part_of_a_longitudinal_sample_series": self.part_of_a_longitudinal_sample_series,
            "notes": self.notes,
            "RNA_seq_method": self.RNA_seq_method,
            "RNA_seq_paired": self.RNA_seq_paired,
            "days_in_space_rr3": self.days_in_space_rr3,
        }


# Output column order
SAMPLE_OUTPUT_COLUMNS = [
    "RR_mission", "OSD_study", "mouse_uid", "sample_name", "extract_name",
    "space_or_ground", "when_was_the_sample_collected", "mouse_sex",
    "mouse_strain", "mouse_genetic_variant", "mouse_source", "organ_sampled",
    "assay_on_organ", "number_of_tech_replicates", "part_of_a_longitudinal_sample_series",
    "notes", "RNA_seq_method", "RNA_seq_paired", "days_in_space_rr3"
]

# Genetic variant normalizations
GENETIC_VARIANT_NORMALIZATIONS = {
    "wild type": "Wild",
    "wt": "Wild",
    "wildtype": "Wild",
    "wild-type": "Wild",
}

# Spaceflight factor value normalizations
SPACEFLIGHT_VALUES = frozenset([
    "space flight", "spaceflight", "flight", "flt", "space", 
    "iss", "orbital", "microgravity", "flown", "micro-gravity"
])

GROUND_VALUES = frozenset([
    "ground control", "ground", "gc", "vivarium", "control",
    "basal", "baseline", "viv", "sham", "non-irradiated",
    "normally loaded", "normally loaded control"
])

# Factor names that indicate ground-based studies (not spaceflight)
GROUND_ANALOG_FACTORS = frozenset([
    "hindlimb unloading", "ionizing radiation", "radiation",
    "irradiation", "particle charge", "age", "genotype"
])

# Values that indicate spaceflight samples specifically
SPACEFLIGHT_CONDITION_VALUES = frozenset([
    "spaceflight", "space flight", "flight", "flown", "flt",
    "orbital", "iss", "micro-gravity", "microgravity"
])

# Values that indicate ground control samples
GROUND_CONDITION_VALUES = frozenset([
    "ground control", "ground", "gc", "vivarium control",
    "basal control", "baseline", "control group", "sham",
    "non-irradiated", "normally loaded control", "normally loaded"
])

# Mission name patterns
MISSION_PATTERNS = [
    (r"RR-?\s*(\d+)", "RR-{}"),  # RR-1, RR-23, etc.
    (r"Rodent Research[- ]?(\d+)", "RR-{}"),
    (r"MHU-?\s*(\d+)", "MHU-{}"),  # MHU-1, MHU-2, etc.
    (r"Mouse Habitat Unit[- ]?(\d+)", "MHU-{}"),
    (r"STS-?\s*(\d+)", "STS-{}"),  # STS-131, etc.
    (r"BION-?\s*M?(\d+)", "BION-M{}"),
]


class SampleExpander:
    """
    Expands OSD IDs into sample-level rows by parsing ISA-Tab files.
    
    This class implements the column mapping rules from the bioinformatics
    intern's workflow to generate properly formatted sample summaries.
    """
    
    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        isa_tab_dir: Optional[Path] = None,
    ):
        """
        Initialize the sample expander.
        
        Args:
            cache_dir: Directory for caching API responses
            isa_tab_dir: Directory for storing ISA-Tab archives
        """
        self.cache_dir = cache_dir or Path("resources/osdr_api/raw")
        self.isa_tab_dir = isa_tab_dir or Path("resources/isa_tab")
        
        # Initialize OSDR client and ISA parser
        self.client = OSDRClient(
            cache_dir=self.cache_dir,
            isa_tab_dir=self.isa_tab_dir,
        )
        self.parser = ISAParser(isa_tab_dir=self.isa_tab_dir)
    
    def expand_osd_to_samples(
        self,
        osd_id: str,
        rr_mission: str = "",
    ) -> List[SampleRow]:
        """
        Expand one OSD ID into multiple sample rows.
        
        Args:
            osd_id: The OSD identifier (e.g., "OSD-467")
            rr_mission: Optional pre-specified mission name
            
        Returns:
            List of SampleRow objects, one per sample
        """
        osd_id = OSDRClient.normalize_osd_id(osd_id)
        
        # Download ISA-Tab if not already cached
        isa_dir = self.client.download_isa_tab(osd_id)
        if not isa_dir:
            print(f"  Warning: Could not download ISA-Tab for {osd_id}")
            return []
        
        # Parse ISA-Tab metadata (now includes assay data with Parameter Values)
        isa_metadata = self.parser.parse(osd_id)
        if not isa_metadata or not isa_metadata.samples:
            print(f"  Warning: No samples found in ISA-Tab for {osd_id}")
            return []
        
        # Extract mission name if not provided
        if not rr_mission:
            rr_mission = self._extract_rr_mission(isa_metadata, osd_id)
        
        # Generate sample rows
        sample_rows = []
        for sample in isa_metadata.samples:
            row = self._create_sample_row(
                sample=sample,
                osd_id=osd_id,
                rr_mission=rr_mission,
                isa_metadata=isa_metadata,
            )
            sample_rows.append(row)
        
        return sample_rows
    
    def expand_multiple_osds(
        self,
        osd_ids: List[Tuple[str, str]],
        show_grouping: bool = True,
    ) -> List[Dict[str, str]]:
        """
        Expand multiple OSD IDs to sample rows.
        
        Args:
            osd_ids: List of tuples (osd_id, rr_mission)
            show_grouping: If True, only show RR_mission and OSD_study on first row of each group
            
        Returns:
            List of row dictionaries ready for CSV output
        """
        all_rows = []
        
        for osd_id, rr_mission in osd_ids:
            print(f"  Processing {osd_id}...")
            
            sample_rows = self.expand_osd_to_samples(osd_id, rr_mission)
            
            if not sample_rows:
                continue
            
            # Convert to dicts and apply grouping
            for i, row in enumerate(sample_rows):
                row_dict = row.to_dict()
                
                # Apply grouping - only show mission/study on first row
                if show_grouping and i > 0:
                    row_dict["RR_mission"] = ""
                    row_dict["OSD_study"] = ""
                
                all_rows.append(row_dict)
        
        return all_rows
    
    def _extract_assay_type(self, filename: str) -> str:
        """Extract assay type from filename."""
        filename_lower = filename.lower()
        
        if "rna-seq" in filename_lower or "rnaseq" in filename_lower or "rna_seq" in filename_lower:
            return "rna-seq"
        elif "transcription" in filename_lower:
            return "rna-seq"
        elif "microarray" in filename_lower:
            return "microarray"
        elif "proteomics" in filename_lower or "protein" in filename_lower:
            return "proteomics"
        elif "metabolomics" in filename_lower or "metabolite" in filename_lower:
            return "metabolomics"
        
        return ""
    
    def _extract_rr_mission(self, isa_metadata, osd_id: str) -> str:
        """
        Extract mission name (RR-1, MHU-3, etc.) from study metadata.
        """
        # Check various metadata fields
        text_to_search = " ".join([
            isa_metadata.investigation_title or "",
            isa_metadata.study_title or "",
            isa_metadata.investigation_description or "",
            isa_metadata.study_description or "",
        ])
        
        for pattern, fmt in MISSION_PATTERNS:
            match = re.search(pattern, text_to_search, re.IGNORECASE)
            if match:
                return fmt.format(match.group(1))
        
        # Check for ground-based study indicators
        text_lower = text_to_search.lower()
        if "hindlimb unloading" in text_lower or "hlu" in text_lower:
            return "Hindlimb_Unloading"
        elif "radiation" in text_lower and "ground" in text_lower:
            return "Ground_Radiation"
        elif "ground" in text_lower and "analog" in text_lower:
            return "Ground_Analog"
        
        return ""
    
    def _create_sample_row(
        self,
        sample,
        osd_id: str,
        rr_mission: str,
        isa_metadata,
    ) -> SampleRow:
        """
        Create a SampleRow from ISA-Tab sample data.
        
        Maps ISA-Tab fields to output columns following the intern's rules.
        """
        row = SampleRow()
        
        # Basic identifiers
        row.RR_mission = rr_mission
        row.OSD_study = osd_id
        row.mouse_uid = sample.source_name or ""
        row.sample_name = sample.sample_name or ""
        
        # Extract characteristics - try multiple source fields
        row.mouse_sex = self._extract_sex(sample)
        
        # Strain can be in Characteristics OR Factor Value (varies by study)
        row.mouse_strain = self._get_characteristic(sample, "strain") or \
                          self._get_factor_value(sample, "strain")
        row.organ_sampled = self._get_characteristic(sample, "organism part") or \
                           self._get_characteristic(sample, "material type")
        row.mouse_source = self._extract_mouse_source(sample)
        
        # Genetic variant - check multiple sources
        row.mouse_genetic_variant = self._extract_genetic_variant(sample)
        
        # Extract spaceflight status from ALL factor values
        row.space_or_ground = self._determine_space_or_ground(sample)
        
        # "After return" logic - only fill if spaceflight
        if row.space_or_ground == "spaceflight":
            row.when_was_the_sample_collected = "after return"
        
        # Get assay data from ISA metadata using the enhanced parser
        assay_sample = isa_metadata.get_assay_data_for_sample(sample.sample_name)
        if assay_sample:
            row.extract_name = assay_sample.extract_name or sample.sample_name
            row.RNA_seq_method = self._normalize_library_selection(
                assay_sample.library_selection
            )
            row.RNA_seq_paired = self._normalize_library_layout(
                assay_sample.library_layout
            )
        else:
            row.extract_name = sample.sample_name
        
        # Get assay type from the assays list
        if isa_metadata.assays:
            for assay in isa_metadata.assays:
                if sample.sample_name in assay.samples:
                    row.assay_on_organ = self._extract_assay_type(assay.filename)
                    break
        
        return row
    
    def _get_characteristic(self, sample, category: str) -> str:
        """Get a characteristic value from a sample."""
        category_lower = category.lower()
        
        for char in sample.characteristics:
            if category_lower in char.category.lower():
                return char.value
        
        return ""
    
    def _extract_sex(self, sample) -> str:
        """
        Extract sex from multiple sources.
        
        Checks:
        1. Characteristics[Sex]
        2. Factor Value[Sex]
        3. Sample name patterns (Male/Female, M/F)
        4. Source name patterns
        """
        # Check characteristics
        sex = self._get_characteristic(sample, "sex")
        if sex:
            return self._normalize_sex(sex)
        
        # Check factor values (some studies store sex as a factor)
        sex_fv = self._get_factor_value(sample, "sex")
        if sex_fv:
            return self._normalize_sex(sex_fv)
        
        # Check sample name for sex indicators
        combined = f"{sample.sample_name} {sample.source_name}".lower()
        
        if any(ind in combined for ind in ["_male", "_m_", "male_", " male", "male ", "male\t"]):
            return "Male"
        if any(ind in combined for ind in ["_female", "_f_", "female_", " female", "female ", "female\t"]):
            return "Female"
        
        return ""
    
    def _normalize_sex(self, value: str) -> str:
        """Normalize sex values to Male/Female."""
        if not value:
            return ""
        
        value_lower = value.lower().strip()
        
        if value_lower in ("male", "m", "males"):
            return "Male"
        if value_lower in ("female", "f", "females"):
            return "Female"
        
        return value.title()
    
    def _extract_genetic_variant(self, sample) -> str:
        """
        Extract genetic variant from multiple sources.
        
        Checks:
        1. Characteristics[Genotype]
        2. Characteristics[Genetic Background]
        3. Factor Value[Genotype]
        4. Sample name patterns for genetic variants
        """
        # Check characteristics for genotype
        genotype = self._get_characteristic(sample, "genotype")
        if genotype:
            return self._normalize_genetic_variant(genotype)
        
        # Check for genetic background
        genetic_bg = self._get_characteristic(sample, "genetic background")
        if genetic_bg:
            return self._normalize_genetic_variant(genetic_bg)
        
        # Check factor values
        genotype_fv = self._get_factor_value(sample, "genotype")
        if genotype_fv:
            return self._normalize_genetic_variant(genotype_fv)
        
        # Check sample name for genetic variant patterns
        combined = f"{sample.sample_name} {sample.source_name}".lower()
        
        # Common genetic variant patterns
        if any(ind in combined for ind in ["_wt_", "_wt-", "wild type", "wildtype"]):
            return "Wild"
        if any(ind in combined for ind in ["_ko_", "_ko-", "knockout"]):
            return "KO"
        if any(ind in combined for ind in ["_het_", "_het-", "heterozygous"]):
            return "Het"
        if any(ind in combined for ind in ["_flox_", "floxed"]):
            return "Flox"
        if any(ind in combined for ind in ["_kd_", "knockdown"]):
            return "KD"
        
        return ""
    
    def _extract_mouse_source(self, sample) -> str:
        """
        Extract mouse source/vendor from multiple sources.
        
        Checks:
        1. Characteristics[Animal Source]
        2. Characteristics[Organism Source]
        3. Comment[Animal Source] (via characteristics parsing)
        4. Factor values
        """
        # Check characteristics first
        source = self._get_characteristic(sample, "animal source")
        if source and source.lower() not in ("not applicable", "n/a", "na", ""):
            return self._normalize_mouse_source(source)
        
        source = self._get_characteristic(sample, "organism source")
        if source and source.lower() not in ("not applicable", "n/a", "na", ""):
            return self._normalize_mouse_source(source)
        
        # Check Comment fields (stored as characteristics by parser)
        for char in sample.characteristics:
            cat_lower = char.category.lower()
            if "animal source" in cat_lower or "organism source" in cat_lower:
                if char.value and char.value.lower() not in ("not applicable", "n/a", "na", ""):
                    return self._normalize_mouse_source(char.value)
        
        return ""
    
    def _normalize_mouse_source(self, value: str) -> str:
        """Normalize mouse source/vendor names."""
        if not value:
            return ""
        
        value_lower = value.lower()
        
        # Normalize common vendor names
        if "jackson" in value_lower:
            return "Jackson Laboratory"
        if "charles river" in value_lower:
            return "Charles River Laboratories"
        if "taconic" in value_lower:
            return "Taconic Biosciences"
        if "harlan" in value_lower:
            return "Harlan Laboratories"
        if "envigo" in value_lower:
            return "Envigo"
        
        return value
    
    def _get_factor_value(self, sample, factor_name: str) -> str:
        """Get a factor value from a sample."""
        factor_lower = factor_name.lower()
        
        for fv in sample.factor_values:
            if factor_lower in fv.factor_name.lower():
                return fv.value
        
        return ""
    
    def _normalize_genetic_variant(self, value: str) -> str:
        """Normalize genetic variant to 'Wild' for wild type."""
        if not value:
            return ""
        
        value_lower = value.lower().strip()
        
        # Check for wild type variations
        if value_lower in GENETIC_VARIANT_NORMALIZATIONS:
            return GENETIC_VARIANT_NORMALIZATIONS[value_lower]
        
        # Check for partial match
        if "wild" in value_lower and "type" in value_lower:
            return "Wild"
        
        return value
    
    def _determine_space_or_ground(self, sample) -> str:
        """
        Determine space_or_ground classification from ALL factor values.
        
        Strategy:
        1. Check for explicit Factor Value[Spaceflight] first
        2. Check all factor values for spaceflight/ground indicators
        3. For ground-analog studies (HLU, radiation), classify as 'ground'
        4. Check sample name for condition codes (FLT, GC, etc.)
        """
        all_factor_values = []
        factor_names = []
        
        # Collect all factor values
        for fv in sample.factor_values:
            factor_names.append(fv.factor_name.lower())
            all_factor_values.append((fv.factor_name.lower(), fv.value.lower()))
        
        # Priority 1: Check explicit Spaceflight factor
        for factor_name, value in all_factor_values:
            if "spaceflight" in factor_name:
                for indicator in SPACEFLIGHT_VALUES:
                    if indicator in value:
                        return "spaceflight"
                for indicator in GROUND_VALUES:
                    if indicator in value:
                        return "ground"
        
        # Priority 2: Check all factor values for spaceflight/ground indicators
        for factor_name, value in all_factor_values:
            # Skip non-condition factors
            if factor_name in ("age", "duration", "time", "dose"):
                continue
            
            # Check for spaceflight indicators in any factor value
            for indicator in SPACEFLIGHT_CONDITION_VALUES:
                if indicator in value:
                    return "spaceflight"
        
        # Priority 3: Check for ground-analog factors (HLU, radiation, etc.)
        for factor_name in factor_names:
            for analog_factor in GROUND_ANALOG_FACTORS:
                if analog_factor in factor_name:
                    # This is a ground-based analog study
                    return "ground"
        
        # Priority 4: Check sample name for condition codes
        sample_name_lower = (sample.sample_name or "").lower()
        source_name_lower = (sample.source_name or "").lower()
        combined = f"{source_name_lower} {sample_name_lower}"
        
        if any(code in combined for code in ["_flt_", "_flt-", "flt_", "-flt-", "flight"]):
            return "spaceflight"
        if any(code in combined for code in ["_gc_", "_gc-", "gc_", "-gc-", "ground", "control"]):
            return "ground"
        
        # Default: if we have any factor values at all but couldn't classify, assume ground
        if all_factor_values:
            return "ground"
        
        return ""
    
    def _normalize_spaceflight_factor(self, value: str) -> str:
        """Normalize spaceflight factor to 'spaceflight' or 'ground'."""
        if not value:
            return ""
        
        value_lower = value.lower().strip()
        
        # Check for spaceflight indicators
        for indicator in SPACEFLIGHT_VALUES:
            if indicator in value_lower:
                return "spaceflight"
        
        # Check for ground control indicators
        for indicator in GROUND_VALUES:
            if indicator in value_lower:
                return "ground"
        
        return value_lower
    
    def _normalize_library_selection(self, value: str) -> str:
        """Normalize library selection method."""
        if not value:
            return ""
        
        value_lower = value.lower()
        
        if "polya" in value_lower or "poly-a" in value_lower or "poly a" in value_lower:
            return "polyA enrichment"
        elif "ribo" in value_lower and "depletion" in value_lower:
            return "ribo-depletion"
        elif "ribo" in value_lower:
            return "ribo-depletion"
        
        return value
    
    def _normalize_library_layout(self, value: str) -> str:
        """Normalize library layout to PAIRED or SINGLE."""
        if not value:
            return ""
        
        value_lower = value.lower()
        
        if "paired" in value_lower:
            return "PAIRED"
        elif "single" in value_lower:
            return "SINGLE"
        
        return value.upper()


def expand_samples_from_csv(
    input_path: Path,
    output_path: Path,
    osd_column: str = "OSD_study",
    mission_column: str = "RR_mission",
) -> int:
    """
    Expand samples from a CSV file containing OSD IDs.
    
    Args:
        input_path: Path to input CSV with OSD IDs
        output_path: Path for output CSV
        osd_column: Name of the column containing OSD IDs
        mission_column: Name of the column containing mission names
        
    Returns:
        Number of samples generated
    """
    expander = SampleExpander()
    
    # Read input CSV
    osd_ids = []
    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            osd_id = row.get(osd_column, "").strip()
            mission = row.get(mission_column, "").strip()
            if osd_id:
                osd_ids.append((osd_id, mission))
    
    print(f"Found {len(osd_ids)} OSD IDs to process")
    
    # Expand all samples
    all_rows = expander.expand_multiple_osds(osd_ids, show_grouping=True)
    
    # Write output CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SAMPLE_OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(all_rows)
    
    print(f"Generated {len(all_rows)} sample rows")
    return len(all_rows)
