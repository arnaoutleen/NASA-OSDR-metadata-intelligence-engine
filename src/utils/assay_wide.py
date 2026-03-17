from __future__ import annotations

from typing import List
import pandas as pd


def assay_long_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert assay_parameters_long into a wide table with one row per
    sample-assay-technology combination.

    This avoids collapsing multiple assays performed on the same sample.
    """
    if df.empty:
        return df.copy()

    id_cols: List[str] = [
        "osd_id",
        "project",
        "mission",
        "sample_id",
        "mouse_id",
        "assay_category",
        "assay_subtype",
        "assay_name",
        "measurement_types",
        "technology_types",
        "device_platforms",
    ]
    id_cols = [c for c in id_cols if c in df.columns]

    wide = (
        df.pivot_table(
            index=id_cols,
            columns="parameter_name",
            values="parameter_value",
            aggfunc=lambda x: " | ".join(
                sorted({str(v) for v in x if pd.notna(v) and str(v) != ""})
            ),
        )
        .reset_index()
    )

    wide.columns.name = None
    return wide
