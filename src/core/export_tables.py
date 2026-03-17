from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
import pandas as pd

from src.utils.export_schema import (
    MOUSE_LEVEL_COLUMNS,
    SAMPLE_LEVEL_COLUMNS,
    ASSAY_PARAMETER_COLUMNS,
)

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
}

ASSAY_PARAMETER_MAP = {
    "rna_sequencing": [
        "Parameter Value[QA Instrument]",
        "Parameter Value[Library Selection]",
        "Parameter Value[Library Layout]",
        "Parameter Value[Stranded]",
        "Parameter Value[Spike-in Mix Number]",
        "Parameter Value[Spike-in Quality Control]",
    ],
    "dna_methylation": [
        "Parameter Value[Library Strategy]",
        "Parameter Value[Library Selection]",
        "Parameter Value[Library Layout]",
        "Parameter Value[Library Type]",
    ],
    "protein_mass_spec": [
        "Parameter Value[Instrument]",
        "Parameter Value[Chromatography]",
        "Parameter Value[Dissociation]",
        "Parameter Value[Pool strategy]",
        "Parameter Value[Analyzer]",
        "Term Source REF",
        "Term Accession Number",
        "MS Assay Name",
    ],
    "rna_methylation": [
        "Parameter Value[QA Assay]",
        "Comment[Extraction Method]",
        "Protocol REF",
        "Parameter Value[Library Strategy]",
        "Parameter Value[Library Selection]",
        "Parameter Value[Library Layout]",
        "Parameter Value[Library Type]",
        "Parameter Value[Sequencing Instrument]",
        "Assay Name",
    ],
    "metabolite_profiling": [
        "Parameter Value[GC/MS instrument]",
        "Term Source REF",
        "Term Accession Number",
        "Parameter Value[GC/MS ion source]",
        "Parameter Value[LC-MS/MS 1-instrument]",
        "Parameter Value[LC-MS/MS 1- ion source]",
        "Parameter Value[LC-MS/MS 1- analyzer]",
        "Parameter Value[LC-MS/MS 2- instrument]",
        "Parameter Value[LC-MS/MS 2- ion source]",
        "Parameter Value[LC-MS/MS 2- analyzer]",
        "Parameter Value[LC-MS/MS 1-Assay name]",
        "Parameter Value[LC-MS/MS 2- Assay Name]",
        "MS Assay Name",
    ],
    "behavior": [
        "Parameter Value[Vector Of The Sequence Of Assays Tests And Treatments Performed In Order]",
        "Parameter Value[Subject Handling Technique]",
        "Parameter Value[Subject Handling Frequency]",
        "Parameter Value[Number Of Screeners Working With Animals For Behavior Assessment]",
        "Parameter Value[Acclimation Time In Testing Room]",
        "Unit",
        "Term Source REF",
        "Term Accession Number",
        "Parameter Value[Phase Of Light Cycle Assay Performed]",
        "Parameter Value[Define Primary Light Source]",
        "Parameter Value[Color Spectrum]",
        "Parameter Value[Temperature Of Testing Room]",
        "Parameter Value[Relative Humidity Of Testing Room]",
        "Parameter Value[External Cues Present During The Assessment]",
        "Parameter Value[Cleaning Solutions Used]",
        "Protocol REF",
        "Parameter Value[Arena Custom Made Or From Vendor]",
        "Parameter Value[Arena Materials And Texture]",
        "Parameter Value[Arena Dimension]",
        "Parameter Value[Arena Measurement]",
        "Parameter Value[Location 1]",
        "Parameter Value[Object At Location 1]",
        "Parameter Value[Location 2]",
        "Parameter Value[Object At Location 2]",
        "Parameter Value[Loading Procedure]",
        "Parameter Value[Phase]",
        "Parameter Value[Interval Since Last Phase]",
        "Parameter Value[Trial Number]",
        "Parameter Value[Trial Duration]",
        "Parameter Value[Rest In Home Cage Between Phases]",
        "Parameter Value[Novel Object]",
        "Parameter Value[Tracking And Analysis System]",
        "Parameter Value[Quantification Method]",
        "Parameter Value[Body Marker Tracked]",
        "Parameter Value[Subset Of Trial Used For Deriving Outcome Measures]",
    ],
    "atpase_activity": [
        "Parameter Value[Plate Reader Instrument]",
    ],
    "calcium_uptake": [
        "Parameter Value[Amount Of Reaction Buffer]",
        "Unit",
        "Term Source REF",
        "Term Accession Number",
        "Parameter Value[Sample Volume]",
        "Parameter Value[Sample Dilution]",
        "Protocol REF",
        "Parameter Value[Volume In Well]",
        "Parameter Value[Plating Method]",
        "Parameter Value[Amount Of ATP]",
        "Parameter Value[Excitation Wavelength]",
        "Parameter Value[Emission Wavelength]",
        "Parameter Value[Read Time]",
        "Parameter Value[Temperature]",
        "Parameter Value[Instrument]",
    ],
    "chromatin_accessibility": [
        "Parameter Value[Library Layout]",
        "Parameter Value[Stranded]",
        "Parameter Value[Cell Count]",
        "Protocol REF",
        "Parameter Value[Sequencing Instrument]",
        "Parameter Value[Base Caller]",
        "Parameter Value[R1 Read Length]",
        "Unit",
        "Term Source REF",
        "Term Accession Number",
        "Parameter Value[R2 Read Length]",
        "Parameter Value[R3 Read Length]",
        "Assay Name",
        "Raw Data File",
        "Parameter Value[Total Cell Count]",
        "Parameter Value[Median Fragments Per Cell]",
    ],
    "echocardiogram": [
        "Parameter Value[Name Of Hardware System]",
        "Parameter Value[Anesthesia]",
        "Parameter Value[Method Of Anesthesia]",
        "Parameter Value[Platform Characteristics]",
        "Parameter Value[Location Of Doppler If Conducted]",
        "Parameter Value[Measurement Mode Of Doppler]",
        "Parameter Value[Orientation Of Probe Placement In Relation To Target If Doppler Conducted]",
        "Protocol REF",
        "Parameter Value[Transducer Type Of Ultrasound Probe]",
        "Parameter Value[Frequency Of Probe]",
        "Parameter Value[Geometric Focus]",
        "Parameter Value[How Many People Analyzed Ultrasonograpy Output]",
        "Parameter Value[Blinded To The Animal Treatments And Experiment Groups]",
        "Parameter Value[Ensemble Collection Of Data Results (E.G. Was Ultrasound Conducted Twice)]",
        "Parameter Value[Name Of Software Data Processor]",
    ],
    "molecular_cellular_imaging": [
        "Parameter Value[Section Thickness]",
    ],
    "protein_quantification": [
        "Parameter Value[Amount Of Protein Loaded]",
        "Unit",
        "Term Source REF",
        "Term Accession Number",
        "Parameter Value[Type Of Gel]",
        "Parameter Value[Voltage]",
        "Parameter Value[Instrument For Gel]",
        "Protocol REF",
        "Parameter Value[Type Of Transfer Membrane]",
        "Parameter Value[Transfer Method]",
        "Parameter Value[Blocking: Chemical]",
        "Parameter Value[Blocking: Duration]",
        "Parameter Value[Number Of Biological Markers]",
        "Parameter Value[Protein Labeled]",
        "Parameter Value[Marker Type]",
        "Parameter Value[Primary Company and Product]",
        "Parameter Value[Chemical Used for Dilution]",
        "Parameter Value[Antigen Host]",
        "Parameter Value[Primary Duration]",
        "Parameter Value[Primary Temperature]",
        "Parameter Value[Wash Buffer]",
        "Parameter Value[Secondary: Fluorophore]",
        "Parameter Value[Secondary Duration]",
        "Parameter Value[Secondary Temperature]",
        "Parameter Value[Imaging Substrate]",
        "Parameter Value[Imaging Substrate Company And Product Number]",
        "Parameter Value[Imaging Method]",
    ],
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


def _project_value(record: Dict[str, Any]) -> Any:
    return _safe_get(record, "project", "mission", "RR_mission")


def classify_assay(record: Dict[str, Any]) -> Tuple[str, str]:
    raw_parts = [
        _as_text(_safe_get(record, "measurement_types", default="")),
        _as_text(_safe_get(record, "technology_types", default="")),
        _as_text(_safe_get(record, "device_platforms", default="")),
        _as_text(_safe_get(record, "assay_on_organ", default="")),
        _as_text(_safe_get(record, "assay_summary", default="")),
        _as_text(_safe_get(record, "assay_names", default="")),
        _as_text(_safe_get(record, "ms_assay_names", default="")),
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


def extract_mouse_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    row = {
        "osd_id": _safe_get(record, "osd_id"),
        "project": _project_value(record),
        "mission": _project_value(record),
        "mouse_id": _safe_get(record, "mouse_id"),
        "source_name": _safe_get(record, "source_name"),
        "strain": _safe_get(record, "mouse_strain", "Characteristics[Strain]"),
        "animal_source": _safe_get(record, "animal_source", "Comment[Animal Source]", "mouse_source"),
        "genotype": _safe_get(record, "genotype", "Characteristics[Genotype]", "mouse_genetic_variant"),
        "sex": _safe_get(record, "mouse_sex", "Characteristics[Sex]"),
        "age": _safe_get(record, "age", "Factor Value[Age]"),
        "spaceflight_status": _safe_get(record, "space_or_ground", "Factor Value[Spaceflight]"),
        "duration": _safe_get(record, "time_in_space", "Parameter Value[duration]"),
        "habitat": _safe_get(record, "habitat", "mouse_habitat"),
        "study_purpose": _safe_get(record, "study_purpose", "study purpose"),
        "n_samples_linked": None,
        "is_pooled_subject": _safe_get(record, "is_pooled_sample", default=False),
        "provenance_source": _safe_get(record, "provenance_source", default="enrichment"),
        "confidence": _safe_get(record, "confidence"),
    }
    cols = list(MOUSE_LEVEL_COLUMNS)
    if "project" in row and "project" not in cols:
        cols = ["project", *cols]
    return {k: row.get(k) for k in cols}


def extract_sample_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    assay_category, assay_subtype = classify_assay(record)
    row = {
        "osd_id": _safe_get(record, "osd_id"),
        "project": _project_value(record),
        "mission": _project_value(record),
        "sample_id": _safe_get(record, "sample_id"),
        "mouse_id": _safe_get(record, "mouse_id"),
        "source_name": _safe_get(record, "source_name"),
        "sample_name": _safe_get(record, "sample_name", "sample_id"),
        "organism": _safe_get(record, "organism"),
        "material_type": _safe_get(record, "material_type"),
        "measurement_types": _as_text(_safe_get(record, "measurement_types")),
        "technology_types": _as_text(_safe_get(record, "technology_types")),
        "device_platforms": _as_text(_safe_get(record, "device_platforms")),
        "assay_category": assay_category,
        "assay_subtype": assay_subtype,
        "assay_name": _safe_get(record, "Assay Name", "assay_name", "MS Assay Name", "assay_names"),
        "raw_data_file": _safe_get(record, "Raw Data File", "raw_data_file", "data_files"),
        "is_rnaseq": assay_category == "rna_sequencing",
        "is_dna_methylation": assay_category == "dna_methylation",
        "is_mass_spec": assay_category == "protein_mass_spec",
        "is_rna_methylation": assay_category == "rna_methylation",
        "is_metabolomics": assay_category == "metabolite_profiling",
        "is_behavior": assay_category == "behavior",
        "is_atpase_activity": assay_category == "atpase_activity",
        "is_calcium_uptake": assay_category == "calcium_uptake",
        "is_atacseq": assay_category == "chromatin_accessibility",
        "is_echocardiogram": assay_category == "echocardiogram",
        "is_imaging": assay_category == "molecular_cellular_imaging",
        "is_western_blot": assay_category == "protein_quantification",
        "assay_assignment_level": _safe_get(record, "assay_assignment_level", default="study"),
        "provenance_source": _safe_get(record, "provenance_source", default="retrieval+enrichment"),
        "confidence": _safe_get(record, "confidence"),
    }
    cols = list(SAMPLE_LEVEL_COLUMNS)
    if "project" in row and "project" not in cols:
        cols = ["project", *cols]
    return {k: row.get(k) for k in cols}


def extract_assay_parameters(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    assay_category, assay_subtype = classify_assay(record)
    parameter_names = ASSAY_PARAMETER_MAP.get(assay_category, [])
    rows: List[Dict[str, Any]] = []

    assay_name = _safe_get(record, "Assay Name", "assay_name", "MS Assay Name", "assay_names")
    measurement_types = _as_text(_safe_get(record, "measurement_types"))
    technology_types = _as_text(_safe_get(record, "technology_types"))
    device_platforms = _as_text(_safe_get(record, "device_platforms"))
    raw_data_file = _safe_get(record, "Raw Data File", "raw_data_file", "data_files")

    for param in parameter_names:
        value = _safe_get(record, param)
        for v in _iter_values(value):
            rows.append({
                "osd_id": _safe_get(record, "osd_id"),
                "project": _project_value(record),
                "mission": _project_value(record),
                "sample_id": _safe_get(record, "sample_id"),
                "mouse_id": _safe_get(record, "mouse_id"),
                "assay_category": assay_category,
                "assay_subtype": assay_subtype,
                "assay_name": assay_name,
                "measurement_types": measurement_types,
                "technology_types": technology_types,
                "device_platforms": device_platforms,
                "parameter_name": param,
                "parameter_value": v,
                "term_source_ref": _safe_get(record, "Term Source REF"),
                "term_accession_number": _safe_get(record, "Term Accession Number"),
                "protocol_ref": _safe_get(record, "Protocol REF"),
                "raw_data_file": raw_data_file,
            })

    return rows


def build_export_tables(records: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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

    if not sample_df.empty and not mouse_df.empty and {"osd_id", "mouse_id", "sample_id"}.issubset(sample_df.columns):
        sample_counts = (
            sample_df.groupby(["osd_id", "mouse_id"], dropna=False)["sample_id"]
            .nunique()
            .reset_index(name="n_samples_linked")
        )
        mouse_df = mouse_df.merge(sample_counts, on=["osd_id", "mouse_id"], how="left", suffixes=("", "_calc"))
        if "n_samples_linked_calc" in mouse_df.columns:
            mouse_df["n_samples_linked"] = mouse_df["n_samples_linked_calc"].combine_first(mouse_df["n_samples_linked"])
            mouse_df = mouse_df.drop(columns=["n_samples_linked_calc"])
        mouse_df = mouse_df.drop_duplicates(subset=["osd_id", "mouse_id"], keep="first").reset_index(drop=True)

    if not sample_df.empty:
        # preserve one row per sample-assay combo instead of collapsing assays together
        dedup_cols = [c for c in ["osd_id", "sample_id", "assay_category", "assay_name", "technology_types", "device_platforms"] if c in sample_df.columns]
        sample_df = sample_df.drop_duplicates(subset=dedup_cols, keep="first").reset_index(drop=True)

    if not assay_df.empty:
        assay_df = assay_df.drop_duplicates().reset_index(drop=True)

    mouse_cols = list(MOUSE_LEVEL_COLUMNS)
    if not mouse_df.empty and "project" in mouse_df.columns and "project" not in mouse_cols:
        mouse_cols = ["project", *mouse_cols]

    sample_cols = list(SAMPLE_LEVEL_COLUMNS)
    if not sample_df.empty and "project" in sample_df.columns and "project" not in sample_cols:
        sample_cols = ["project", *sample_cols]

    assay_cols = list(ASSAY_PARAMETER_COLUMNS)
    assay_extra_cols = ["project", "measurement_types", "technology_types", "device_platforms"]
    for col in assay_extra_cols:
        if col in assay_df.columns and col not in assay_cols:
            assay_cols.append(col)

    mouse_df = mouse_df.reindex(columns=mouse_cols)
    sample_df = sample_df.reindex(columns=sample_cols)
    assay_df = assay_df.reindex(columns=assay_cols)

    return mouse_df, sample_df, assay_df
