from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
import pandas as pd

from src.utils.export_schema import MOUSE_LEVEL_COLUMNS, SAMPLE_LEVEL_COLUMNS, ASSAY_PARAMETER_COLUMNS

ASSAY_CATEGORY_MAP = {
    "rna sequencing": "rna_sequencing",
    "targeted transcriptome sequencing": "rna_sequencing",
    "dna methylation": "dna_methylation",
    "whole genome bisulfite sequencing": "dna_methylation",
    "whole transcriptome bisulfite sequencing": "dna_methylation",
    "protein profiling": "protein_mass_spec",
    "phosphoprotein profiling": "protein_mass_spec",
    "mass spectrometry": "protein_mass_spec",
    "metabolite profiling": "metabolite_profiling",
    "rna methylation": "rna_methylation",
    "behavior": "behavior",
    "ethovision": "behavior",
    "atpase activity": "atpase_activity",
    "calcium uptake": "calcium_uptake",
    "chromatin accessibility": "chromatin_accessibility",
    "atac-seq": "chromatin_accessibility",
    "echocardiogram": "echocardiogram",
    "molecular-cellular-imaging": "molecular_cellular_imaging",
    "microscopy": "molecular_cellular_imaging",
    "protein quantification": "protein_quantification",
    "western blot": "protein_quantification",
    # SampleExpander assay_on_organ / assay_category flat values
    "rna-seq": "rna_sequencing",
    "mass-spec": "protein_mass_spec",
    "metabolomics": "metabolite_profiling",
    "rna-methylation": "rna_methylation",
    "dna-methylation": "dna_methylation",
    "atac_seq": "chromatin_accessibility",
    "rna_seq": "rna_sequencing",
    "mass_spec": "protein_mass_spec",
    "rna_methylation": "rna_methylation",
    "dna_methylation": "dna_methylation",
    "western_blot": "protein_quantification",
    "western-blot": "protein_quantification",
    "atpase": "atpase_activity",
    "calcium_uptake": "calcium_uptake",
}

# Maps canonical ISA parameter name -> SampleRow flat field name, per assay category.
# extract_assay_parameters tries the canonical key first, then the flat SampleRow key.
_SAMPLEROW_FALLBACKS: Dict[str, Dict[str, str]] = {
    "rna_sequencing": {
        "Parameter Value[QA Instrument]":            "rnaseq_qa_instrument",
        "Parameter Value[Library Selection]":        "RNA_seq_method",
        "Parameter Value[Library Layout]":           "RNA_seq_paired",
        "Parameter Value[Stranded]":                 "rnaseq_stranded",
        "Parameter Value[Spike-in Mix Number]":      "rnaseq_spikein_mix",
        "Parameter Value[Spike-in Quality Control]": "rnaseq_spikein_qc",
    },
    "dna_methylation": {
        "Parameter Value[Library Strategy]":  "dnameth_library_strategy",
        "Parameter Value[Library Selection]": "dnameth_library_selection",
        "Parameter Value[Library Layout]":    "dnameth_library_layout",
        "Parameter Value[Library Type]":      "dnameth_library_type",
    },
    "protein_mass_spec": {
        "Parameter Value[Instrument]":     "ms_instrument",
        "Parameter Value[Chromatography]": "ms_chromatography",
        "Parameter Value[Dissociation]":   "ms_dissociation",
        "Parameter Value[Pool strategy]":  "ms_pool_strategy",
        "Parameter Value[Analyzer]":       "ms_analyzer",
        "MS Assay Name":                   "ms_assay_name",
    },
    "rna_methylation": {
        "Parameter Value[QA Assay]":              "rnameth_qa_assay",
        "Comment[Extraction Method]":             "rnameth_extraction_method",
        "Parameter Value[Library Strategy]":      "rnameth_library_strategy",
        "Parameter Value[Library Selection]":     "rnameth_library_selection",
        "Parameter Value[Library Layout]":        "rnameth_library_layout",
        "Parameter Value[Library Type]":          "rnameth_library_type",
        "Parameter Value[Sequencing Instrument]": "rnameth_sequencing_instrument",
        "Assay Name":                             "rnameth_assay_name",
    },
    "metabolite_profiling": {
        "Parameter Value[GC/MS instrument]":       "metab_gcms_instrument",
        "Parameter Value[GC/MS ion source]":       "metab_gcms_ion_source",
        "Parameter Value[LC-MS/MS 1-instrument]":  "metab_lcms1_instrument",
        "Parameter Value[LC-MS/MS 1- ion source]": "metab_lcms1_ion_source",
        "Parameter Value[LC-MS/MS 1- analyzer]":   "metab_lcms1_analyzer",
        "Parameter Value[LC-MS/MS 2- instrument]": "metab_lcms2_instrument",
        "Parameter Value[LC-MS/MS 2- ion source]": "metab_lcms2_ion_source",
        "Parameter Value[LC-MS/MS 2- analyzer]":   "metab_lcms2_analyzer",
        "Parameter Value[LC-MS/MS 1-Assay name]":  "metab_lcms1_assay_name",
        "Parameter Value[LC-MS/MS 2- Assay Name]": "metab_lcms2_assay_name",
        "MS Assay Name":                           "metab_ms_assay_name",
    },
    "behavior": {
        "Parameter Value[Vector Of The Sequence Of Assays Tests And Treatments Performed In Order]": "behavior_vector",
        "Parameter Value[Subject Handling Technique]":   "behavior_handling_technique",
        "Parameter Value[Subject Handling Frequency]":   "behavior_handling_frequency",
        "Parameter Value[Number Of Screeners Working With Animals For Behavior Assessment]": "behavior_num_screeners",
        "Parameter Value[Acclimation Time In Testing Room]":     "behavior_acclimation_time",
        "Parameter Value[Phase Of Light Cycle Assay Performed]": "behavior_light_cycle_phase",
        "Parameter Value[Define Primary Light Source]":          "behavior_light_source",
        "Parameter Value[Color Spectrum]":                       "behavior_color_spectrum",
        "Parameter Value[Temperature Of Testing Room]":          "behavior_room_temperature",
        "Parameter Value[Relative Humidity Of Testing Room]":    "behavior_room_humidity",
        "Parameter Value[External Cues Present During The Assessment]": "behavior_external_cues",
        "Parameter Value[Cleaning Solutions Used]":              "behavior_cleaning_solutions",
        "Parameter Value[Arena Custom Made Or From Vendor]":     "behavior_arena_source",
        "Parameter Value[Arena Materials And Texture]":          "behavior_arena_materials",
        "Parameter Value[Arena Dimension]":                      "behavior_arena_dimension",
        "Parameter Value[Arena Measurement]":                    "behavior_arena_measurement",
        "Parameter Value[Location 1]":                           "behavior_location_1",
        "Parameter Value[Object At Location 1]":                 "behavior_object_at_location_1",
        "Parameter Value[Location 2]":                           "behavior_location_2",
        "Parameter Value[Object At Location 2]":                 "behavior_object_at_location_2",
        "Parameter Value[Loading Procedure]":                    "behavior_loading_procedure",
        "Parameter Value[Phase]":                                "behavior_phase",
        "Parameter Value[Interval Since Last Phase]":            "behavior_interval_since_last_phase",
        "Parameter Value[Trial Number]":                         "behavior_trial_number",
        "Parameter Value[Trial Duration]":                       "behavior_trial_duration",
        "Parameter Value[Rest In Home Cage Between Phases]":     "behavior_rest_in_home_cage",
        "Parameter Value[Novel Object]":                         "behavior_novel_object",
        "Parameter Value[Tracking And Analysis System]":         "behavior_tracking_system",
        "Parameter Value[Quantification Method]":                "behavior_quantification_method",
        "Parameter Value[Body Marker Tracked]":                  "behavior_body_marker_tracked",
        "Parameter Value[Subset Of Trial Used For Deriving Outcome Measures]": "behavior_subset_of_trial",
    },
    "atpase_activity": {
        "Parameter Value[Plate Reader Instrument]": "atpase_plate_reader",
    },
    "calcium_uptake": {
        "Parameter Value[Amount Of Reaction Buffer]": "calcium_reaction_buffer",
        "Parameter Value[Sample Volume]":             "calcium_sample_volume",
        "Parameter Value[Sample Dilution]":           "calcium_sample_dilution",
        "Parameter Value[Volume In Well]":            "calcium_volume_in_well",
        "Parameter Value[Plating Method]":            "calcium_plating_method",
        "Parameter Value[Amount Of ATP]":             "calcium_atp_amount",
        "Parameter Value[Excitation Wavelength]":     "calcium_excitation_wavelength",
        "Parameter Value[Emission Wavelength]":       "calcium_emission_wavelength",
        "Parameter Value[Read Time]":                 "calcium_read_time",
        "Parameter Value[Temperature]":               "calcium_temperature",
        "Parameter Value[Instrument]":                "calcium_instrument",
    },
    "chromatin_accessibility": {
        "Parameter Value[Library Layout]":            "atac_library_layout",
        "Parameter Value[Stranded]":                  "atac_stranded",
        "Parameter Value[Cell Count]":                "atac_cell_count",
        "Parameter Value[Sequencing Instrument]":     "atac_sequencing_instrument",
        "Parameter Value[Base Caller]":               "atac_base_caller",
        "Parameter Value[R1 Read Length]":            "atac_r1_read_length",
        "Parameter Value[R2 Read Length]":            "atac_r2_read_length",
        "Parameter Value[R3 Read Length]":            "atac_r3_read_length",
        "Assay Name":                                 "atac_assay_name",
        "Parameter Value[Total Cell Count]":          "atac_total_cell_count",
        "Parameter Value[Median Fragments Per Cell]": "atac_median_fragments_per_cell",
    },
    "echocardiogram": {
        "Parameter Value[Name Of Hardware System]":   "echo_hardware_system",
        "Parameter Value[Anesthesia]":                "echo_anesthesia",
        "Parameter Value[Method Of Anesthesia]":      "echo_anesthesia_method",
        "Parameter Value[Platform Characteristics]":  "echo_platform_characteristics",
        "Parameter Value[Location Of Doppler If Conducted]":     "echo_doppler_location",
        "Parameter Value[Measurement Mode Of Doppler]":          "echo_doppler_mode",
        "Parameter Value[Orientation Of Probe Placement In Relation To Target If Doppler Conducted]": "echo_probe_orientation",
        "Parameter Value[Transducer Type Of Ultrasound Probe]":  "echo_transducer_type",
        "Parameter Value[Frequency Of Probe]":                   "echo_probe_frequency",
        "Parameter Value[Geometric Focus]":                      "echo_geometric_focus",
        "Parameter Value[How Many People Analyzed Ultrasonograpy Output]": "echo_num_analyzers",
        "Parameter Value[Blinded To The Animal Treatments And Experiment Groups]": "echo_blinded",
        "Parameter Value[Ensemble Collection Of Data Results (E.G. Was Ultrasound Conducted Twice)]": "echo_ensemble_collection",
        "Parameter Value[Name Of Software Data Processor]":      "echo_software",
    },
    "molecular_cellular_imaging": {
        "Parameter Value[Section Thickness]": "microscopy_section_thickness",
    },
    "protein_quantification": {
        "Parameter Value[Amount Of Protein Loaded]":  "wb_protein_amount",
        "Parameter Value[Type Of Gel]":               "wb_gel_type",
        "Parameter Value[Voltage]":                   "wb_voltage",
        "Parameter Value[Instrument For Gel]":        "wb_gel_instrument",
        "Parameter Value[Type Of Transfer Membrane]": "wb_membrane_type",
        "Parameter Value[Transfer Method]":           "wb_transfer_method",
        "Parameter Value[Blocking: Chemical]":        "wb_blocking_chemical",
        "Parameter Value[Blocking: Duration]":        "wb_blocking_duration",
        "Parameter Value[Number Of Biological Markers]": "wb_num_markers",
        "Parameter Value[Protein Labeled]":           "wb_protein_labeled",
        "Parameter Value[Marker Type]":               "wb_marker_type",
        "Parameter Value[Primary Company and Product]": "wb_primary_antibody",
        "Parameter Value[Chemical Used for Dilution]": "wb_dilution_chemical",
        "Parameter Value[Antigen Host]":              "wb_antigen_host",
        "Parameter Value[Primary Duration]":          "wb_primary_duration",
        "Parameter Value[Primary Temperature]":       "wb_primary_temperature",
        "Parameter Value[Wash Buffer]":               "wb_wash_buffer",
        "Parameter Value[Secondary: Fluorophore]":    "wb_secondary_fluorophore",
        "Parameter Value[Secondary Duration]":        "wb_secondary_duration",
        "Parameter Value[Secondary Temperature]":     "wb_secondary_temperature",
        "Parameter Value[Imaging Substrate]":         "wb_imaging_substrate",
        "Parameter Value[Imaging Substrate Company And Product Number]": "wb_imaging_substrate_product",
        "Parameter Value[Imaging Method]":            "wb_imaging_method",
    },
}


def _safe_get(record: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in record and record[key] not in [None, "", [], {}]:
            return record[key]
    return default


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " | ".join(str(v) for v in value if v not in [None, ""])
    return str(value)


def _iter_values(value: Any) -> Iterable[Any]:
    if value in [None, "", [], {}]:
        return []
    if isinstance(value, list):
        return [v for v in value if v not in [None, ""]]
    return [value]


def classify_assay(record: Dict[str, Any]) -> Tuple[str, str]:
    raw_parts = [
        _as_text(_safe_get(record, "measurement_types", default="")),
        _as_text(_safe_get(record, "technology_types", default="")),
        _as_text(_safe_get(record, "device_platforms", default="")),
        _as_text(_safe_get(record, "assay_on_organ", default="")),
        _as_text(_safe_get(record, "assay_summary", default="")),
        _as_text(_safe_get(record, "assay_names", default="")),
        _as_text(_safe_get(record, "ms_assay_names", default="")),
        # SampleExpander already computed assay_category; include it for direct matching
        _as_text(_safe_get(record, "assay_category", default="")),
    ]
    raw_text = " | ".join(raw_parts).lower()

    matched: List[Tuple[str, str]] = []
    for phrase, category in ASSAY_CATEGORY_MAP.items():
        if phrase in raw_text:
            matched.append((phrase, category))

    if matched:
        phrase, category = matched[0]
        return category, phrase

    return "unknown", "unknown"


def _project(record: Dict[str, Any]) -> Any:
    return _safe_get(record, "project", "mission", "RR_mission", "OSD_study", default="")


def extract_mouse_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    row = {
        "osd_id":            _safe_get(record, "osd_id", "OSD_study"),
        "project":           _project(record),
        "mission":           _safe_get(record, "mission", "RR_mission"),
        "mouse_id":          _safe_get(record, "mouse_id", "mouse_uid"),
        "source_name":       _safe_get(record, "source_name", "mouse_uid"),
        "strain":            _safe_get(record, "mouse_strain", "Characteristics[Strain]", "strain"),
        "animal_source":     _safe_get(record, "mouse_source", "animal_source",
                                       "Comment[Animal Source]"),
        "genotype":          _safe_get(record, "mouse_genetic_variant", "genotype",
                                       "Characteristics[Genotype]"),
        "sex":               _safe_get(record, "mouse_sex", "Characteristics[Sex]", "sex"),
        "age":               _safe_get(record, "age", "Factor Value[Age]"),
        "spaceflight_status": _safe_get(record, "space_or_ground", "Factor Value[Spaceflight]"),
        "duration":          _safe_get(record, "days_in_space_rr3", "duration",
                                       "Parameter Value[duration]"),
        "habitat":           _safe_get(record, "habitat", "mouse_habitat"),
        "study_purpose":     _safe_get(record, "study_purpose", "study purpose"),
        "n_samples_linked":  None,
        "is_pooled_subject": _safe_get(record, "is_pooled_sample", default=False),
        "provenance_source": _safe_get(record, "provenance_source", default="isa_tab"),
        "confidence":        _safe_get(record, "confidence"),
    }
    return {k: row.get(k) for k in MOUSE_LEVEL_COLUMNS}


def extract_sample_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    assay_category, assay_subtype = classify_assay(record)
    row = {
        "osd_id":            _safe_get(record, "osd_id", "OSD_study"),
        "project":           _project(record),
        "mission":           _safe_get(record, "mission", "RR_mission"),
        "sample_id":         _safe_get(record, "sample_id", "sample_name"),
        "mouse_id":          _safe_get(record, "mouse_id", "mouse_uid"),
        "source_name":       _safe_get(record, "source_name", "mouse_uid"),
        "sample_name":       _safe_get(record, "sample_name"),
        "mouse_strain":      _safe_get(record, "mouse_strain", "Characteristics[Strain]"),
        "mouse_sex":         _safe_get(record, "mouse_sex", "Characteristics[Sex]"),
        "age":               _safe_get(record, "age", "Factor Value[Age]"),
        "space_or_ground":   _safe_get(record, "space_or_ground", "Factor Value[Spaceflight]"),
        "organism":          _safe_get(record, "organism"),
        "material_type":     _safe_get(record, "material_type", "organ_sampled"),
        "measurement_types": _as_text(_safe_get(record, "measurement_types")),
        "technology_types":  _as_text(_safe_get(record, "technology_types")),
        "device_platforms":  _as_text(_safe_get(record, "device_platforms")),
        "assay_category":    assay_category,
        "assay_subtype":     assay_subtype,
        "assay_name":        _safe_get(record, "Assay Name", "assay_name", "MS Assay Name",
                                       "ms_assay_name", "assay_names"),
        "raw_data_file":     _safe_get(record, "Raw Data File", "raw_data_file", "data_files"),
        "is_rnaseq":         assay_category == "rna_sequencing",
        "is_dna_methylation": assay_category == "dna_methylation",
        "is_mass_spec":      assay_category == "protein_mass_spec",
        "is_rna_methylation": assay_category == "rna_methylation",
        "is_metabolomics":   assay_category == "metabolite_profiling",
        "is_behavior":       assay_category == "behavior",
        "is_atpase_activity": assay_category == "atpase_activity",
        "is_calcium_uptake": assay_category == "calcium_uptake",
        "is_atacseq":        assay_category == "chromatin_accessibility",
        "is_echocardiogram": assay_category == "echocardiogram",
        "is_imaging":        assay_category == "molecular_cellular_imaging",
        "is_western_blot":   assay_category == "protein_quantification",
        "assay_assignment_level": _safe_get(record, "assay_assignment_level", default="sample"),
        "provenance_source": _safe_get(record, "provenance_source", default="isa_tab"),
        "confidence":        _safe_get(record, "confidence"),
    }
    return {k: row.get(k) for k in SAMPLE_LEVEL_COLUMNS}


def extract_assay_parameters(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    assay_category, assay_subtype = classify_assay(record)
    fallbacks = _SAMPLEROW_FALLBACKS.get(assay_category, {})
    parameter_names = list(fallbacks.keys()) if fallbacks else []

    rows: List[Dict[str, Any]] = []
    base = {
        "osd_id":            _safe_get(record, "osd_id", "OSD_study"),
        "project":           _project(record),
        "mission":           _safe_get(record, "mission", "RR_mission"),
        "sample_id":         _safe_get(record, "sample_id", "sample_name"),
        "mouse_id":          _safe_get(record, "mouse_id", "mouse_uid"),
        "assay_category":    assay_category,
        "assay_subtype":     assay_subtype,
        "assay_name":        _safe_get(record, "Assay Name", "assay_name", "MS Assay Name",
                                       "ms_assay_name", "assay_names"),
        "measurement_types": _as_text(_safe_get(record, "measurement_types")),
        "technology_types":  _as_text(_safe_get(record, "technology_types")),
        "device_platforms":  _as_text(_safe_get(record, "device_platforms")),
        "term_source_ref":   _safe_get(record, "Term Source REF"),
        "term_accession_number": _safe_get(record, "Term Accession Number"),
        "protocol_ref":      _safe_get(record, "Protocol REF"),
        "raw_data_file":     _safe_get(record, "Raw Data File", "raw_data_file"),
    }

    for param in parameter_names:
        flat_key = fallbacks.get(param, "")
        # Try the ISA canonical key first, then the SampleRow flat field
        value = _safe_get(record, param) or (
            _safe_get(record, flat_key) if flat_key else None
        )
        for v in _iter_values(value):
            rows.append({**base, "parameter_name": param, "parameter_value": v})

    return rows


def build_export_tables(
    records: List[Dict[str, Any]],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    mouse_rows: List[Dict[str, Any]] = []
    sample_rows: List[Dict[str, Any]] = []
    assay_rows: List[Dict[str, Any]] = []

    for rec in records:
        mouse_rows.append(extract_mouse_metadata(rec))
        sample_rows.append(extract_sample_metadata(rec))
        assay_rows.extend(extract_assay_parameters(rec))

    mouse_df = pd.DataFrame(mouse_rows)
    sample_df = pd.DataFrame(sample_rows)
    assay_df = pd.DataFrame(assay_rows)

    if not sample_df.empty:
        sample_df = sample_df.drop_duplicates(
            subset=["osd_id", "sample_id", "assay_category", "assay_name"]
        ).reset_index(drop=True)

    if not mouse_df.empty:
        counts = (
            sample_df.groupby(["osd_id", "mouse_id"], dropna=False)["sample_id"]
            .count()
            .reset_index(name="n_samples_linked")
        )
        mouse_df = mouse_df.merge(
            counts, on=["osd_id", "mouse_id"], how="left", suffixes=("", "_calc")
        )
        if "n_samples_linked_calc" in mouse_df.columns:
            mouse_df["n_samples_linked"] = mouse_df["n_samples_linked_calc"].fillna(
                mouse_df["n_samples_linked"]
            )
            mouse_df = mouse_df.drop(columns=["n_samples_linked_calc"])
        mouse_df = mouse_df.drop_duplicates(subset=["osd_id", "mouse_id"]).reset_index(drop=True)

    mouse_df = mouse_df.reindex(columns=MOUSE_LEVEL_COLUMNS)
    sample_df = sample_df.reindex(columns=SAMPLE_LEVEL_COLUMNS)
    assay_df = assay_df.reindex(columns=ASSAY_PARAMETER_COLUMNS)

    return mouse_df, sample_df, assay_df
