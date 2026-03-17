
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

from src.core.export_tables import build_export_tables
from src.utils.assay_wide import assay_long_to_wide

RAW_ASSAY_COLUMNS = ["measurement_types", "technology_types", "device_platforms"]
SAMPLE_PROV_COLUMNS = [
    "mouse_sex",
    "mouse_strain",
    "space_or_ground",
    "organ_sampled",
    "time_in_space",
    "assay_on_organ",
    "habitat",
]
STUDY_PROV_COLUMNS = [
    "assay_on_organ",
    "Has_RNAseq",
    "n_RNAseq_mice",
    "n_mice_total",
    "age_when_sent_to_space",
    "study purpose",
]
FINAL_COLUMN_ORDER = [
    "osd_id",
    "project",
    "sample_id",
    "source_name",
    "RR_mission",
    "mouse_id",
    "mouse_sex",
    "mouse_strain",
    "mouse_genetic_variant",
    "age",
    "age_when_sent_to_space",
    "space_or_ground",
    "organ_sampled",
    "habitat",
    "mouse_source",
    "n_mice_total",
    "n_RNAseq_mice",
    "measurement_types",
    "technology_types",
    "device_platforms",
    "data_files",
    "assay_on_organ",
    "assay_summary",
    "assay_assignment_level",
    "Has_RNAseq",
    "is_rnaseq",
    "is_scrnaseq",
    "is_mass_spec",
    "is_wgbs",
    "is_wtbs",
    "study purpose",
    "organism",
    "material_type",
]

def _first_non_empty(values: Iterable[Any]) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and pd.isna(value):
            continue
        text = str(value).strip()
        if text and text.lower() != "nan":
            return value
    return ""

def _coalesce_columns(df: pd.DataFrame, left: str, right: str, target: str | None = None) -> pd.DataFrame:
    target = target or left
    if left not in df.columns and right not in df.columns:
        return df
    if left not in df.columns:
        df[target] = df[right]
        return df
    if right not in df.columns:
        df[target] = df[left]
        return df
    df[target] = df[left].where(df[left].notna() & (df[left].astype(str).str.strip() != ""), df[right])
    if target != left and left in df.columns:
        df.drop(columns=[left], inplace=True)
    return df

def _normalize_pipe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        tokens = [str(x).strip() for x in value if str(x).strip()]
        return " | ".join(dict.fromkeys(tokens))
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    return text

def _truthy(text: str) -> bool:
    return text.strip().lower() in {"yes", "true", "1", "y"}

def _derive_assay_summary(row: pd.Series) -> Tuple[str, str]:
    pieces: List[str] = []
    for col in RAW_ASSAY_COLUMNS:
        value = _normalize_pipe_text(row.get(col, ""))
        if value:
            pieces.append(value)
    study_value = _normalize_pipe_text(row.get("assay_on_organ", ""))
    if study_value:
        pieces.append(study_value)

    seen = []
    for piece in pieces:
        for token in [x.strip() for x in piece.replace(",", "|").split("|") if x.strip()]:
            if token not in seen:
                seen.append(token)
    summary = " | ".join(seen)

    has_sample_level = any(_normalize_pipe_text(row.get(col, "")) for col in RAW_ASSAY_COLUMNS)
    if has_sample_level:
        level = "sample"
    elif study_value:
        level = "study"
    else:
        level = "unknown"
    return summary, level

def _derive_boolean_flags(summary: str) -> Dict[str, str]:
    text = summary.lower()
    return {
        "is_rnaseq": "Yes" if any(token in text for token in ["rna seq", "rna-seq", "rna sequencing", "transcriptom"]) else "No",
        "is_scrnaseq": "Yes" if any(token in text for token in ["single cell", "scrna", "single-cell"]) else "No",
        "is_mass_spec": "Yes" if any(token in text for token in ["mass spect", "proteom", "metabolom"]) else "No",
        "is_wgbs": "Yes" if "whole genome bisulfite" in text or "wgbs" in text else "No",
        "is_wtbs": "Yes" if "whole transcriptome bisulfite" in text or "wtbs" in text else "No",
    }

def _collect_nodes(obj: Any, bucket: List[Dict[str, Any]]) -> None:
    if isinstance(obj, dict):
        bucket.append(obj)
        for value in obj.values():
            _collect_nodes(value, bucket)
    elif isinstance(obj, list):
        for item in obj:
            _collect_nodes(item, bucket)

def _parse_provenance(provenance_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not provenance_path.exists():
        return pd.DataFrame(), pd.DataFrame()
    with open(provenance_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    nodes: List[Dict[str, Any]] = []
    _collect_nodes(data, nodes)
    sample_rows: List[Dict[str, Any]] = []
    study_rows: List[Dict[str, Any]] = []
    for node in nodes:
        osd_id = node.get("osd_id") or node.get("osd") or node.get("study_id") or ""
        sample_id = node.get("sample_id") or node.get("sample_name") or ""
        if not osd_id:
            continue
        payload = {"osd_id": osd_id, "sample_id": sample_id}
        for key in SAMPLE_PROV_COLUMNS:
            if key in node:
                payload[key] = node[key]
        for key in STUDY_PROV_COLUMNS:
            if key in node:
                payload[key] = node[key]
        if not any(key in payload for key in SAMPLE_PROV_COLUMNS + STUDY_PROV_COLUMNS):
            continue
        if sample_id in {"", "_study_level_", "study_level"}:
            study_rows.append({k: v for k, v in payload.items() if k != "sample_id"})
        else:
            sample_rows.append(payload)
    def _collapse(df: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
        if df.empty:
            return df
        value_cols = [c for c in df.columns if c not in group_cols]
        return df.groupby(group_cols, dropna=False)[value_cols].agg(_first_non_empty).reset_index()
    return _collapse(pd.DataFrame(sample_rows), ["osd_id", "sample_id"]), _collapse(pd.DataFrame(study_rows), ["osd_id"])

def _merged_records_from_outputs(
    retrieved_csv_path: Path,
    pipeline_csv_path: Path,
    provenance_json_path: Path,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    retrieval_df = pd.read_csv(retrieved_csv_path)
    pipeline_df = pd.read_csv(pipeline_csv_path)
    sample_prov_df, study_prov_df = _parse_provenance(provenance_json_path)

    merged = pipeline_df.merge(
        retrieval_df,
        on=[col for col in ["osd_id", "sample_id"] if col in retrieval_df.columns and col in pipeline_df.columns],
        how="left",
        suffixes=("", "_retrieved"),
    )
    for col in ["source_name", "RR_mission", "organism", "material_type", "measurement_types", "technology_types", "device_platforms", "data_files"]:
        retrieved_col = f"{col}_retrieved"
        if retrieved_col in merged.columns:
            merged = _coalesce_columns(merged, col, retrieved_col)
            if retrieved_col in merged.columns:
                merged.drop(columns=[retrieved_col], inplace=True)

    if not sample_prov_df.empty:
        merged = merged.merge(sample_prov_df, on=["osd_id", "sample_id"], how="left", suffixes=("", "_prov_sample"))
        for col in SAMPLE_PROV_COLUMNS:
            prov_col = f"{col}_prov_sample"
            if prov_col in merged.columns:
                merged = _coalesce_columns(merged, col, prov_col)
                if prov_col in merged.columns:
                    merged.drop(columns=[prov_col], inplace=True)

    if not study_prov_df.empty:
        merged = merged.merge(study_prov_df, on=["osd_id"], how="left", suffixes=("", "_prov_study"))
        for col in STUDY_PROV_COLUMNS:
            prov_col = f"{col}_prov_study"
            if prov_col in merged.columns:
                merged = _coalesce_columns(merged, col, prov_col)
                if prov_col in merged.columns:
                    merged.drop(columns=[prov_col], inplace=True)

    if "mouse_genetic_variant" in merged.columns:
        if "mouse_strain" not in merged.columns:
            merged["mouse_strain"] = merged["mouse_genetic_variant"]
        else:
            merged["mouse_strain"] = merged["mouse_strain"].where(
                merged["mouse_strain"].notna() & (merged["mouse_strain"].astype(str).str.strip() != ""),
                merged["mouse_genetic_variant"],
            )

    # derive project everywhere
    if "project" not in merged.columns:
        merged["project"] = ""
    merged["project"] = merged["project"].where(
        merged["project"].notna() & (merged["project"].astype(str).str.strip() != ""),
        merged.get("RR_mission", pd.Series([""] * len(merged)))
    )
    if merged["project"].astype(str).str.strip().eq("").all():
        unique_osds = sorted(set(str(x) for x in merged["osd_id"].dropna().unique()))
        fallback = unique_osds[0] if len(unique_osds) == 1 else "multi_osd"
        merged["project"] = fallback

    assay_summaries = merged.apply(_derive_assay_summary, axis=1, result_type="expand")
    assay_summaries.columns = ["assay_summary", "assay_assignment_level"]
    merged = pd.concat([merged, assay_summaries], axis=1)

    derived_flags = merged["assay_summary"].apply(_derive_boolean_flags).apply(pd.Series)
    merged = pd.concat([merged, derived_flags], axis=1)

    if "Has_RNAseq" in merged.columns:
        merged["Has_RNAseq"] = merged.apply(
            lambda row: "Yes" if _truthy(str(row.get("Has_RNAseq", ""))) or row.get("is_rnaseq", "No") == "Yes" else "No",
            axis=1,
        )
    else:
        merged["Has_RNAseq"] = merged["is_rnaseq"]

    for col in RAW_ASSAY_COLUMNS + ["assay_on_organ", "assay_summary"]:
        if col in merged.columns:
            merged[col] = merged[col].apply(_normalize_pipe_text)

    for col in FINAL_COLUMN_ORDER:
        if col not in merged.columns:
            merged[col] = ""

    extra_cols = [c for c in merged.columns if c not in FINAL_COLUMN_ORDER]
    merged = merged[FINAL_COLUMN_ORDER + extra_cols].copy()
    return merged, merged.to_dict(orient="records")

def write_all_export_tables(
    retrieved_csv_path: str | Path,
    pipeline_csv_path: str | Path,
    provenance_json_path: str | Path,
    output_dir: str | Path,
) -> Dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    merged_df, final_records = _merged_records_from_outputs(
        retrieved_csv_path=Path(retrieved_csv_path),
        pipeline_csv_path=Path(pipeline_csv_path),
        provenance_json_path=Path(provenance_json_path),
    )

    enriched_samples_path = output_dir / "enriched_samples.csv"
    merged_df.to_csv(enriched_samples_path, index=False)

    mouse_df, sample_df, assay_df = build_export_tables(final_records)
    mouse_path = output_dir / "mouse_metadata.csv"
    sample_path = output_dir / "sample_metadata.csv"
    assay_long_path = output_dir / "assay_parameters_long.csv"
    assay_wide_path = output_dir / "assay_parameters_wide.csv"

    mouse_df.to_csv(mouse_path, index=False)
    sample_df.to_csv(sample_path, index=False)
    assay_df.to_csv(assay_long_path, index=False)
    assay_long_to_wide(assay_df).to_csv(assay_wide_path, index=False)

    return {
        "enriched_samples": enriched_samples_path,
        "mouse_metadata": mouse_path,
        "sample_metadata": sample_path,
        "assay_parameters_long": assay_long_path,
        "assay_parameters_wide": assay_wide_path,
    }
