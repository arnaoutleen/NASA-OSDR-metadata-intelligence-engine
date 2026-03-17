from __future__ import annotations

import pandas as pd


ASSAY_WIDE_ID_COLUMNS = [
    "osd_id",
    "project",
    "sample_id",
    "mouse_id",
    "assay_category",
    "assay_subtype",
    "assay_name",
    "measurement_types",
    "technology_types",
    "device_platforms",
]


def _collapse_values(values: pd.Series) -> str:
    seen: list[str] = []
    for value in values:
        if pd.isna(value):
            continue
        text = str(value).strip()
        if text and text not in seen:
            seen.append(text)
    return " | ".join(seen)



def assay_long_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Convert long assay parameters into one row per sample-assay combination."""
    if df.empty:
        return df.copy()

    id_cols = [col for col in ASSAY_WIDE_ID_COLUMNS if col in df.columns]
    wide = (
        df.pivot_table(
            index=id_cols,
            columns="parameter_name",
            values="parameter_value",
            aggfunc=_collapse_values,
            dropna=False,
        )
        .reset_index()
    )
    wide.columns.name = None
    return wide
