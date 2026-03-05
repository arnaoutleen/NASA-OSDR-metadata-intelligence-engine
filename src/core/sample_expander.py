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
from typing import Dict, List, Optional, Tuple

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
    # RNA sequencing (targeted transcriptome sequencing)
    rnaseq_qa_instrument: str = ""
    rnaseq_stranded: str = ""
    rnaseq_spikein_mix: str = ""
    rnaseq_spikein_qc: str = ""
    # DNA methylation
    dnameth_library_strategy: str = ""
    dnameth_library_selection: str = ""
    dnameth_library_layout: str = ""
    dnameth_library_type: str = ""
    # Protein profiling / mass spec (Orbitrap, phosphoprotein profiling)
    ms_instrument: str = ""
    ms_chromatography: str = ""
    ms_dissociation: str = ""
    ms_pool_strategy: str = ""
    ms_analyzer: str = ""
    ms_assay_name: str = ""
    # RNA methylation
    rnameth_qa_assay: str = ""
    rnameth_extraction_method: str = ""
    rnameth_library_strategy: str = ""
    rnameth_library_selection: str = ""
    rnameth_library_layout: str = ""
    rnameth_library_type: str = ""
    rnameth_sequencing_instrument: str = ""
    rnameth_assay_name: str = ""
    # Metabolite profiling
    metab_gcms_instrument: str = ""
    metab_gcms_ion_source: str = ""
    metab_lcms1_instrument: str = ""
    metab_lcms1_ion_source: str = ""
    metab_lcms1_analyzer: str = ""
    metab_lcms2_instrument: str = ""
    metab_lcms2_ion_source: str = ""
    metab_lcms2_analyzer: str = ""
    metab_lcms1_assay_name: str = ""
    metab_lcms2_assay_name: str = ""
    metab_ms_assay_name: str = ""
    # Behavior (Ethovision)
    behavior_vector: str = ""
    behavior_handling_technique: str = ""
    behavior_handling_frequency: str = ""
    behavior_num_screeners: str = ""
    behavior_acclimation_time: str = ""
    behavior_light_cycle_phase: str = ""
    behavior_light_source: str = ""
    behavior_color_spectrum: str = ""
    behavior_room_temperature: str = ""
    behavior_room_humidity: str = ""
    behavior_external_cues: str = ""
    behavior_cleaning_solutions: str = ""
    behavior_arena_source: str = ""
    behavior_arena_materials: str = ""
    behavior_arena_dimension: str = ""
    behavior_arena_measurement: str = ""
    behavior_location_1: str = ""
    behavior_object_at_location_1: str = ""
    behavior_location_2: str = ""
    behavior_object_at_location_2: str = ""
    behavior_loading_procedure: str = ""
    behavior_phase: str = ""
    behavior_interval_since_last_phase: str = ""
    behavior_trial_number: str = ""
    behavior_trial_duration: str = ""
    behavior_rest_in_home_cage: str = ""
    behavior_novel_object: str = ""
    behavior_tracking_system: str = ""
    behavior_quantification_method: str = ""
    behavior_body_marker_tracked: str = ""
    behavior_subset_of_trial: str = ""
    # ATPase activity (spectrophotometry)
    atpase_plate_reader: str = ""
    # Calcium uptake
    calcium_reaction_buffer: str = ""
    calcium_sample_volume: str = ""
    calcium_sample_dilution: str = ""
    calcium_volume_in_well: str = ""
    calcium_plating_method: str = ""
    calcium_atp_amount: str = ""
    calcium_excitation_wavelength: str = ""
    calcium_emission_wavelength: str = ""
    calcium_read_time: str = ""
    calcium_temperature: str = ""
    calcium_instrument: str = ""
    # Chromatin accessibility (ATAC-seq)
    atac_library_layout: str = ""
    atac_stranded: str = ""
    atac_cell_count: str = ""
    atac_sequencing_instrument: str = ""
    atac_base_caller: str = ""
    atac_r1_read_length: str = ""
    atac_r2_read_length: str = ""
    atac_r3_read_length: str = ""
    atac_assay_name: str = ""
    atac_total_cell_count: str = ""
    atac_median_fragments_per_cell: str = ""
    # Echocardiogram
    echo_hardware_system: str = ""
    echo_anesthesia: str = ""
    echo_anesthesia_method: str = ""
    echo_platform_characteristics: str = ""
    echo_doppler_location: str = ""
    echo_doppler_mode: str = ""
    echo_probe_orientation: str = ""
    echo_transducer_type: str = ""
    echo_probe_frequency: str = ""
    echo_geometric_focus: str = ""
    echo_num_analyzers: str = ""
    echo_blinded: str = ""
    echo_ensemble_collection: str = ""
    echo_software: str = ""
    # Molecular-cellular imaging (microscopy)
    microscopy_section_thickness: str = ""
    # Protein quantification (western blot)
    wb_protein_amount: str = ""
    wb_gel_type: str = ""
    wb_voltage: str = ""
    wb_gel_instrument: str = ""
    wb_membrane_type: str = ""
    wb_transfer_method: str = ""
    wb_blocking_chemical: str = ""
    wb_blocking_duration: str = ""
    wb_num_markers: str = ""
    wb_protein_labeled: str = ""
    wb_marker_type: str = ""
    wb_primary_antibody: str = ""
    wb_dilution_chemical: str = ""
    wb_antigen_host: str = ""
    wb_primary_duration: str = ""
    wb_primary_temperature: str = ""
    wb_wash_buffer: str = ""
    wb_secondary_fluorophore: str = ""
    wb_secondary_duration: str = ""
    wb_secondary_temperature: str = ""
    wb_imaging_substrate: str = ""
    wb_imaging_substrate_product: str = ""
    wb_imaging_method: str = ""

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for CSV output."""
        return {f: getattr(self, f) for f in SAMPLE_OUTPUT_COLUMNS}


# Output column order
SAMPLE_OUTPUT_COLUMNS = [
    # Core sample identity and study metadata
    "RR_mission", "OSD_study", "mouse_uid", "sample_name", "extract_name",
    "space_or_ground", "when_was_the_sample_collected", "mouse_sex",
    "mouse_strain", "mouse_genetic_variant", "mouse_source", "organ_sampled",
    "assay_on_organ", "number_of_tech_replicates", "part_of_a_longitudinal_sample_series",
    "notes", "days_in_space_rr3",
    # RNA sequencing (targeted transcriptome sequencing)
    "RNA_seq_method", "RNA_seq_paired",
    "rnaseq_qa_instrument", "rnaseq_stranded", "rnaseq_spikein_mix", "rnaseq_spikein_qc",
    # DNA methylation
    "dnameth_library_strategy", "dnameth_library_selection",
    "dnameth_library_layout", "dnameth_library_type",
    # Protein profiling / mass spec (Orbitrap, phosphoprotein profiling)
    "ms_instrument", "ms_chromatography", "ms_dissociation",
    "ms_pool_strategy", "ms_analyzer", "ms_assay_name",
    # RNA methylation
    "rnameth_qa_assay", "rnameth_extraction_method", "rnameth_library_strategy",
    "rnameth_library_selection", "rnameth_library_layout", "rnameth_library_type",
    "rnameth_sequencing_instrument", "rnameth_assay_name",
    # Metabolite profiling
    "metab_gcms_instrument", "metab_gcms_ion_source",
    "metab_lcms1_instrument", "metab_lcms1_ion_source", "metab_lcms1_analyzer",
    "metab_lcms2_instrument", "metab_lcms2_ion_source", "metab_lcms2_analyzer",
    "metab_lcms1_assay_name", "metab_lcms2_assay_name", "metab_ms_assay_name",
    # Behavior (Ethovision)
    "behavior_vector", "behavior_handling_technique", "behavior_handling_frequency",
    "behavior_num_screeners", "behavior_acclimation_time", "behavior_light_cycle_phase",
    "behavior_light_source", "behavior_color_spectrum", "behavior_room_temperature",
    "behavior_room_humidity", "behavior_external_cues", "behavior_cleaning_solutions",
    "behavior_arena_source", "behavior_arena_materials", "behavior_arena_dimension",
    "behavior_arena_measurement", "behavior_location_1", "behavior_object_at_location_1",
    "behavior_location_2", "behavior_object_at_location_2", "behavior_loading_procedure",
    "behavior_phase", "behavior_interval_since_last_phase", "behavior_trial_number",
    "behavior_trial_duration", "behavior_rest_in_home_cage", "behavior_novel_object",
    "behavior_tracking_system", "behavior_quantification_method",
    "behavior_body_marker_tracked", "behavior_subset_of_trial",
    # ATPase activity (spectrophotometry)
    "atpase_plate_reader",
    # Calcium uptake
    "calcium_reaction_buffer", "calcium_sample_volume", "calcium_sample_dilution",
    "calcium_volume_in_well", "calcium_plating_method", "calcium_atp_amount",
    "calcium_excitation_wavelength", "calcium_emission_wavelength",
    "calcium_read_time", "calcium_temperature", "calcium_instrument",
    # Chromatin accessibility (ATAC-seq)
    "atac_library_layout", "atac_stranded", "atac_cell_count",
    "atac_sequencing_instrument", "atac_base_caller",
    "atac_r1_read_length", "atac_r2_read_length", "atac_r3_read_length",
    "atac_assay_name", "atac_total_cell_count", "atac_median_fragments_per_cell",
    # Echocardiogram
    "echo_hardware_system", "echo_anesthesia", "echo_anesthesia_method",
    "echo_platform_characteristics", "echo_doppler_location", "echo_doppler_mode",
    "echo_probe_orientation", "echo_transducer_type", "echo_probe_frequency",
    "echo_geometric_focus", "echo_num_analyzers", "echo_blinded",
    "echo_ensemble_collection", "echo_software",
    # Molecular-cellular imaging (microscopy)
    "microscopy_section_thickness",
    # Protein quantification (western blot)
    "wb_protein_amount", "wb_gel_type", "wb_voltage", "wb_gel_instrument",
    "wb_membrane_type", "wb_transfer_method", "wb_blocking_chemical", "wb_blocking_duration",
    "wb_num_markers", "wb_protein_labeled", "wb_marker_type", "wb_primary_antibody",
    "wb_dilution_chemical", "wb_antigen_host", "wb_primary_duration", "wb_primary_temperature",
    "wb_wash_buffer", "wb_secondary_fluorophore", "wb_secondary_duration",
    "wb_secondary_temperature", "wb_imaging_substrate", "wb_imaging_substrate_product",
    "wb_imaging_method",
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
        """Extract a human-readable assay type label from filename."""
        filename_lower = filename.lower()

        if "rna-seq" in filename_lower or "rnaseq" in filename_lower or "rna_seq" in filename_lower:
            return "rna-seq"
        elif "transcription" in filename_lower and "methylation" not in filename_lower:
            return "rna-seq"
        elif "targeted-transcriptome" in filename_lower or "targeted_transcriptome" in filename_lower:
            return "rna-seq"
        elif "atac" in filename_lower:
            return "atac-seq"
        elif "methylation" in filename_lower and "rna" in filename_lower:
            return "rna-methylation"
        elif "methylation" in filename_lower or "bisulfite" in filename_lower:
            return "dna-methylation"
        elif "microarray" in filename_lower:
            return "microarray"
        elif "mass-spec" in filename_lower or "mass_spec" in filename_lower or "orbitrap" in filename_lower:
            return "mass-spec"
        elif "phospho" in filename_lower:
            return "mass-spec"
        elif "proteomics" in filename_lower or "protein-profiling" in filename_lower or "protein_profiling" in filename_lower:
            return "mass-spec"
        elif "metabolomics" in filename_lower or "metabolite" in filename_lower:
            return "metabolomics"
        elif "behavior" in filename_lower or "ethovision" in filename_lower:
            return "behavior"
        elif "atpase" in filename_lower or "enzymatic" in filename_lower:
            return "atpase"
        elif "calcium" in filename_lower:
            return "calcium-uptake"
        elif "echocardio" in filename_lower or "ultrasound" in filename_lower:
            return "echocardiogram"
        elif "imaging" in filename_lower or "microscopy" in filename_lower or "histology" in filename_lower:
            return "microscopy"
        elif "western" in filename_lower or "protein-quantification" in filename_lower or "protein_quantification" in filename_lower:
            return "western-blot"

        return ""

    def _detect_assay_category(self, assay) -> str:
        """
        Detect the functional assay category from an ISAAssay object.

        Uses measurement_type and technology_type (parsed from the filename)
        to return a canonical category string used to populate assay-specific
        SampleRow fields.
        """
        mtype = assay.measurement_type.lower()
        ttype = assay.technology_type.lower()
        fname = assay.filename.lower()
        combined = f"{mtype} {ttype} {fname}"

        if "atac" in combined or "chromatin accessibility" in combined:
            return "atac_seq"
        if "rna methylation" in combined or "epitranscriptomics" in combined or \
                ("methylation" in combined and "rna" in combined and "dna" not in combined):
            return "rna_methylation"
        if "dna methylation" in combined or "bisulfite" in combined or \
                ("methylation" in combined and "dna" in combined):
            return "dna_methylation"
        if "transcription profiling" in combined or "rna sequencing" in combined or \
                "rna-seq" in combined or "rnaseq" in combined or \
                "targeted transcriptome" in combined:
            return "rna_seq"
        # Metabolomics checked before mass_spec: metabolite profiling studies also use mass spec
        if "metabolite profiling" in combined or "metabolomics" in combined:
            return "metabolomics"
        if "orbitrap" in combined or "mass spectrometry" in combined or \
                "protein profiling" in combined or "phosphoprotein" in combined or \
                "proteomics" in combined:
            return "mass_spec"
        if "behavior" in combined or "ethovision" in combined:
            return "behavior"
        if "atpase" in combined or "enzymatic activity" in combined:
            return "atpase"
        if "calcium" in combined:
            return "calcium_uptake"
        if "echocardio" in combined or "ultrasound" in combined:
            return "echocardiogram"
        if "imaging" in combined or "microscopy" in combined or "histology" in combined:
            return "microscopy"
        if "protein quantification" in combined or "western blot" in combined or \
                "western-blot" in combined:
            return "western_blot"

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
        
        # Get assay data and assay type
        assay_sample = isa_metadata.get_assay_data_for_sample(sample.sample_name)
        matched_assay = None
        if isa_metadata.assays:
            for assay in isa_metadata.assays:
                if sample.sample_name in assay.samples:
                    matched_assay = assay
                    row.assay_on_organ = self._extract_assay_type(assay.filename)
                    break

        if assay_sample:
            row.extract_name = assay_sample.extract_name or sample.sample_name
            pv = assay_sample.parameter_values
            cv = assay_sample.comment_values

            # Determine assay category for conditional field population
            category = self._detect_assay_category(matched_assay) if matched_assay else ""

            if category == "rna_seq":
                row.RNA_seq_method = self._normalize_library_selection(
                    assay_sample.library_selection
                )
                row.RNA_seq_paired = self._normalize_library_layout(
                    assay_sample.library_layout
                )
                row.rnaseq_qa_instrument = pv.get("QA Instrument", "")
                row.rnaseq_stranded = pv.get("Stranded", "")
                row.rnaseq_spikein_mix = pv.get("Spike-in Mix Number", "")
                row.rnaseq_spikein_qc = pv.get("Spike-in Quality Control", "")

            elif category == "dna_methylation":
                row.dnameth_library_strategy = pv.get("Library Strategy", "")
                row.dnameth_library_selection = pv.get("Library Selection", "")
                row.dnameth_library_layout = pv.get("Library Layout", "")
                row.dnameth_library_type = pv.get("Library Type", "")

            elif category == "mass_spec":
                row.ms_instrument = pv.get("Instrument", "")
                row.ms_chromatography = pv.get("Chromatography", "")
                row.ms_dissociation = pv.get("Dissociation", "")
                row.ms_pool_strategy = pv.get("Pool strategy", "")
                row.ms_analyzer = pv.get("Analyzer", "")
                row.ms_assay_name = assay_sample.ms_assay_name

            elif category == "rna_methylation":
                row.rnameth_qa_assay = pv.get("QA Assay", "")
                row.rnameth_extraction_method = cv.get("Extraction Method", "")
                row.rnameth_library_strategy = pv.get("Library Strategy", "")
                row.rnameth_library_selection = pv.get("Library Selection", "")
                row.rnameth_library_layout = pv.get("Library Layout", "")
                row.rnameth_library_type = pv.get("Library Type", "")
                row.rnameth_sequencing_instrument = pv.get("Sequencing Instrument", "")
                row.rnameth_assay_name = assay_sample.assay_name

            elif category == "metabolomics":
                row.metab_gcms_instrument = pv.get("GC/MS instrument", "")
                row.metab_gcms_ion_source = pv.get("GC/MS ion source", "")
                row.metab_lcms1_instrument = pv.get("LC-MS/MS 1-instrument", "")
                row.metab_lcms1_ion_source = pv.get("LC-MS/MS 1- ion source", "")
                row.metab_lcms1_analyzer = pv.get("LC-MS/MS 1- analyzer", "")
                row.metab_lcms2_instrument = pv.get("LC-MS/MS 2- instrument", "")
                row.metab_lcms2_ion_source = pv.get("LC-MS/MS 2- ion source", "")
                row.metab_lcms2_analyzer = pv.get("LC-MS/MS 2- analyzer", "")
                row.metab_lcms1_assay_name = pv.get("LC-MS/MS 1-Assay name", "")
                row.metab_lcms2_assay_name = pv.get("LC-MS/MS 2- Assay Name", "")
                row.metab_ms_assay_name = assay_sample.ms_assay_name

            elif category == "behavior":
                row.behavior_vector = pv.get(
                    "Vector Of The Sequence Of Assays Tests And Treatments Performed In Order", "")
                row.behavior_handling_technique = pv.get("Subject Handling Technique", "")
                row.behavior_handling_frequency = pv.get("Subject Handling Frequency", "")
                row.behavior_num_screeners = pv.get(
                    "Number Of Screeners Working With Animals For Behavior Assessment", "")
                row.behavior_acclimation_time = pv.get("Acclimation Time In Testing Room", "")
                row.behavior_light_cycle_phase = pv.get("Phase Of Light Cycle Assay Performed", "")
                row.behavior_light_source = pv.get("Define Primary Light Source", "")
                row.behavior_color_spectrum = pv.get("Color Spectrum", "")
                row.behavior_room_temperature = pv.get("Temperature Of Testing Room", "")
                row.behavior_room_humidity = pv.get("Relative Humidity Of Testing Room", "")
                row.behavior_external_cues = pv.get("External Cues Present During The Assessment", "")
                row.behavior_cleaning_solutions = pv.get("Cleaning Solutions Used", "")
                row.behavior_arena_source = pv.get("Arena Custom Made Or From Vendor", "")
                row.behavior_arena_materials = pv.get("Arena Materials And Texture", "")
                row.behavior_arena_dimension = pv.get("Arena Dimension", "")
                row.behavior_arena_measurement = pv.get("Arena Measurement", "")
                row.behavior_location_1 = pv.get("Location 1", "")
                row.behavior_object_at_location_1 = pv.get("Object At Location 1", "")
                row.behavior_location_2 = pv.get("Location 2", "")
                row.behavior_object_at_location_2 = pv.get("Object At Location 2", "")
                row.behavior_loading_procedure = pv.get("Loading Procedure", "")
                row.behavior_phase = pv.get("Phase", "")
                row.behavior_interval_since_last_phase = pv.get("Interval Since Last Phase", "")
                row.behavior_trial_number = pv.get("Trial Number", "")
                row.behavior_trial_duration = pv.get("Trial Duration", "")
                row.behavior_rest_in_home_cage = pv.get("Rest In Home Cage Between Phases", "")
                row.behavior_novel_object = pv.get("Novel Object", "")
                row.behavior_tracking_system = pv.get("Tracking And Analysis System", "")
                row.behavior_quantification_method = pv.get("Quantification Method", "")
                row.behavior_body_marker_tracked = pv.get("Body Marker Tracked", "")
                row.behavior_subset_of_trial = pv.get(
                    "Subset Of Trial Used For Deriving Outcome Measures", "")

            elif category == "atpase":
                row.atpase_plate_reader = pv.get("Plate Reader Instrument", "")

            elif category == "calcium_uptake":
                row.calcium_reaction_buffer = pv.get("Amount Of Reaction Buffer", "")
                row.calcium_sample_volume = pv.get("Sample Volume", "")
                row.calcium_sample_dilution = pv.get("Sample Dilution", "")
                row.calcium_volume_in_well = pv.get("Volume In Well", "")
                row.calcium_plating_method = pv.get("Plating Method", "")
                row.calcium_atp_amount = pv.get("Amount Of ATP", "")
                row.calcium_excitation_wavelength = pv.get("Excitation Wavelength", "")
                row.calcium_emission_wavelength = pv.get("Emission Wavelength", "")
                row.calcium_read_time = pv.get("Read Time", "")
                row.calcium_temperature = pv.get("Temperature", "")
                row.calcium_instrument = pv.get("Instrument", "")

            elif category == "atac_seq":
                row.atac_library_layout = pv.get("Library Layout", "")
                row.atac_stranded = pv.get("Stranded", "")
                row.atac_cell_count = pv.get("Cell Count", "")
                row.atac_sequencing_instrument = pv.get("Sequencing Instrument", "")
                row.atac_base_caller = pv.get("Base Caller", "")
                row.atac_r1_read_length = pv.get("R1 Read Length", "")
                row.atac_r2_read_length = pv.get("R2 Read Length", "")
                row.atac_r3_read_length = pv.get("R3 Read Length", "")
                row.atac_assay_name = assay_sample.assay_name
                row.atac_total_cell_count = pv.get("Total Cell Count", "")
                row.atac_median_fragments_per_cell = pv.get("Median Fragments Per Cell", "")

            elif category == "echocardiogram":
                row.echo_hardware_system = pv.get("Name Of Hardware System", "")
                row.echo_anesthesia = pv.get("Anesthesia", "")
                row.echo_anesthesia_method = pv.get("Method Of Anesthesia", "")
                row.echo_platform_characteristics = pv.get("Platform Characteristics", "")
                row.echo_doppler_location = pv.get("Location Of Doppler If Conducted", "")
                row.echo_doppler_mode = pv.get("Measurement Mode Of Doppler", "")
                row.echo_probe_orientation = pv.get(
                    "Orientation Of Probe Placement In Relation To Target If Doppler Conducted", "")
                row.echo_transducer_type = pv.get("Transducer Type Of Ultrasound Probe", "")
                row.echo_probe_frequency = pv.get("Frequency Of Probe", "")
                row.echo_geometric_focus = pv.get("Geometric Focus", "")
                row.echo_num_analyzers = pv.get("How Many People Analyzed Ultrasonograpy Output", "")
                row.echo_blinded = pv.get(
                    "Blinded To The Animal Treatments And Experiment Groups", "")
                row.echo_ensemble_collection = pv.get(
                    "Ensemble Collection Of Data Results (E.G. Was Ultrasound Conducted Twice)", "")
                row.echo_software = pv.get("Name Of Software Data Processor", "")

            elif category == "microscopy":
                row.microscopy_section_thickness = pv.get("Section Thickness", "")

            elif category == "western_blot":
                row.wb_protein_amount = pv.get("Amount Of Protein Loaded", "")
                row.wb_gel_type = pv.get("Type Of Gel", "")
                row.wb_voltage = pv.get("Voltage", "")
                row.wb_gel_instrument = pv.get("Instrument For Gel", "")
                row.wb_membrane_type = pv.get("Type Of Transfer Membrane", "")
                row.wb_transfer_method = pv.get("Transfer Method", "")
                row.wb_blocking_chemical = pv.get("Blocking: Chemical", "")
                row.wb_blocking_duration = pv.get("Blocking: Duration", "")
                row.wb_num_markers = pv.get("Number Of Biological Markers", "")
                row.wb_protein_labeled = pv.get("Protein Labeled", "")
                row.wb_marker_type = pv.get("Marker Type", "")
                row.wb_primary_antibody = pv.get("Primary Company and Product", "")
                row.wb_dilution_chemical = pv.get("Chemical Used for Dilution", "")
                row.wb_antigen_host = pv.get("Antigen Host", "")
                row.wb_primary_duration = pv.get("Primary Duration", "")
                row.wb_primary_temperature = pv.get("Primary Temperature", "")
                row.wb_wash_buffer = pv.get("Wash Buffer", "")
                row.wb_secondary_fluorophore = pv.get("Secondary: Fluorophore", "")
                row.wb_secondary_duration = pv.get("Secondary Duration", "")
                row.wb_secondary_temperature = pv.get("Secondary Temperature", "")
                row.wb_imaging_substrate = pv.get("Imaging Substrate", "")
                row.wb_imaging_substrate_product = pv.get(
                    "Imaging Substrate Company And Product Number", "")
                row.wb_imaging_method = pv.get("Imaging Method", "")

        else:
            row.extract_name = sample.sample_name

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
