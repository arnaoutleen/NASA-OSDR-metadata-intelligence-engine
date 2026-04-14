
from __future__ import annotations

MOUSE_LEVEL_COLUMNS = [
    "osd_ids",           # list of all OSDs this mouse appears in  e.g. "OSD-48 | OSD-168"
    "pulled_at",
    "mission",           # payload / rocket  e.g. "SpaceX-4"  (was called payload)
    "project",           # research project  e.g. "RR-1"       (was called mission)
    "mouse_id",
    "source_name",
    "strain",
    "animal_source",
    "genotype",
    "sex",
    "age",
    "spaceflight_status",
    "duration",
    "habitat",
    "light_cycle",
    "diet",
    "feeding_schedule",
    "euthanasia_method",
    "study_purpose",
    "n_samples_linked",
    "is_pooled_subject",
    "provenance_source",
    "confidence",
    # Cross-study inventory columns
    "sample_inventory",        # JSON list: [{sample_id, osd_id, material_type, assay_category}]
    "organ_assay_inventory",   # JSON list: [{organ, assays, osd_id}] — one entry per organ×OSD
    "informativeness_score",
    "informativeness_rank",
]

SAMPLE_LEVEL_COLUMNS = [
    "osd_id",
    "pulled_at",
    "mission",           # payload / rocket  e.g. "SpaceX-4"
    "project",           # research project  e.g. "RR-1"
    "sample_id",
    "mouse_id",
    "source_name",
    "sample_name",
    "mouse_strain",
    "mouse_sex",
    "age",
    "space_or_ground",
    "organism",
    "material_type",
    "measurement_types",
    "technology_types",
    "device_platforms",
    "assay_category",
    "assay_subtype",
    "assay_name",
    "raw_data_file",
    "is_rnaseq",
    "is_dna_methylation",
    "is_mass_spec",
    "is_rna_methylation",
    "is_metabolomics",
    "is_behavior",
    "is_atpase_activity",
    "is_calcium_uptake",
    "is_atacseq",
    "is_echocardiogram",
    "is_imaging",
    "is_western_blot",
    "is_bone_microstructure",
    "is_microarray",
    "assay_assignment_level",
    "provenance_source",
    "confidence",
]

ASSAY_PARAMETER_COLUMNS = [
    "osd_id",
    "pulled_at",
    "mission",           # payload / rocket  e.g. "SpaceX-4"
    "project",           # research project  e.g. "RR-1"
    "sample_id",
    "mouse_id",
    "assay_category",
    "assay_subtype",
    "assay_name",
    "measurement_types",
    "technology_types",
    "device_platforms",
    "parameter_name",
    "parameter_value",
    "term_source_ref",
    "term_accession_number",
    "protocol_ref",
    "raw_data_file",
]
