
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
    return _safe_get(record, "project", "mission", "RR_mission", default="")


def extract_mouse_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    row = {
        "osd_id": _safe_get(record, "osd_id"),
        "project": _project(record),
        "mission": _safe_get(record, "mission", "RR_mission"),
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
    return {k: row.get(k) for k in MOUSE_LEVEL_COLUMNS}


def extract_sample_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    assay_category, assay_subtype = classify_assay(record)
    row = {
        "osd_id": _safe_get(record, "osd_id"),
        "project": _project(record),
        "mission": _safe_get(record, "mission", "RR_mission"),
        "sample_id": _safe_get(record, "sample_id"),
        "mouse_id": _safe_get(record, "mouse_id"),
        "source_name": _safe_get(record, "source_name"),
        "sample_name": _safe_get(record, "sample_name", "sample_id"),
        "mouse_strain": _safe_get(record, "mouse_strain", "Characteristics[Strain]"),
        "mouse_sex": _safe_get(record, "mouse_sex", "Characteristics[Sex]"),
        "age": _safe_get(record, "age", "Factor Value[Age]"),
        "space_or_ground": _safe_get(record, "space_or_ground", "Factor Value[Spaceflight]"),
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
    return {k: row.get(k) for k in SAMPLE_LEVEL_COLUMNS}


def extract_assay_parameters(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    assay_category, assay_subtype = classify_assay(record)
    parameter_names = ASSAY_PARAMETER_MAP.get(assay_category, [])
    rows: List[Dict[str, Any]] = []

    for param in parameter_names:
        value = _safe_get(record, param)
        for v in _iter_values(value):
            rows.append({
                "osd_id": _safe_get(record, "osd_id"),
                "project": _project(record),
                "mission": _safe_get(record, "mission", "RR_mission"),
                "sample_id": _safe_get(record, "sample_id"),
                "mouse_id": _safe_get(record, "mouse_id"),
                "assay_category": assay_category,
                "assay_subtype": assay_subtype,
                "assay_name": _safe_get(record, "Assay Name", "assay_name", "MS Assay Name", "assay_names"),
                "measurement_types": _as_text(_safe_get(record, "measurement_types")),
                "technology_types": _as_text(_safe_get(record, "technology_types")),
                "device_platforms": _as_text(_safe_get(record, "device_platforms")),
                "parameter_name": param,
                "parameter_value": v,
                "term_source_ref": _safe_get(record, "Term Source REF"),
                "term_accession_number": _safe_get(record, "Term Accession Number"),
                "protocol_ref": _safe_get(record, "Protocol REF"),
                "raw_data_file": _safe_get(record, "Raw Data File", "raw_data_file"),
            })

    return rows


def build_export_tables(records: List[Dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
        sample_df = sample_df.drop_duplicates(subset=["osd_id", "sample_id", "assay_category", "assay_name"]).reset_index(drop=True)

    if not mouse_df.empty:
        counts = (sample_df.groupby(["osd_id", "mouse_id"], dropna=False)["sample_id"].count()
                  .reset_index(name="n_samples_linked"))
        mouse_df = mouse_df.merge(counts, on=["osd_id", "mouse_id"], how="left", suffixes=("", "_calc"))
        if "n_samples_linked_calc" in mouse_df.columns:
            mouse_df["n_samples_linked"] = mouse_df["n_samples_linked_calc"].fillna(mouse_df["n_samples_linked"])
            mouse_df = mouse_df.drop(columns=["n_samples_linked_calc"])
        mouse_df = mouse_df.drop_duplicates(subset=["osd_id", "mouse_id"]).reset_index(drop=True)

    mouse_df = mouse_df.reindex(columns=MOUSE_LEVEL_COLUMNS)
    sample_df = sample_df.reindex(columns=SAMPLE_LEVEL_COLUMNS)
    assay_df = assay_df.reindex(columns=ASSAY_PARAMETER_COLUMNS)

    return mouse_df, sample_df, assay_df
