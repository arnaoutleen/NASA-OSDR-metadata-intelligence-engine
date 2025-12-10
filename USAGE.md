# Usage Guide

**Complete instructions for the NASA OSDR Metadata Intelligence Engine**

---

## Table of Contents

1. [Installation](#1-installation)
2. [Quick Start](#2-quick-start)
3. [CLI Reference](#3-cli-reference)
4. [Input File Formats](#4-input-file-formats)
5. [Output Files](#5-output-files)
6. [Understanding Provenance](#6-understanding-provenance)
7. [Validation Reports](#7-validation-reports)
8. [Common Workflows](#8-common-workflows)
9. [Troubleshooting](#9-troubleshooting)
10. [Advanced Usage](#10-advanced-usage)

---

## 1. Installation

### Prerequisites

- Python 3.11 or higher (3.12 recommended)
- pip package manager
- Network access to NASA OSDR APIs

### Setup

```bash
# Clone the repository
git clone https://github.com/yeshasvikamma/NASA-OSDR-metadata-intelligence-engine.git
cd NASA-OSDR-metadata-intelligence-engine

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### Verify Installation

```bash
python -c "import requests, pandas; print('Dependencies OK')"
```

---

## 2. Quick Start

### Basic Enrichment

Process a CSV file and generate enriched outputs. Data is fetched automatically on-demand:

```bash
python -m cli.process_csv resources/test_inputs/demo/realworld_rodent_research.csv --validate
```

The pipeline will automatically:
1. Download ISA-Tab archives from NASA for any studies it needs
2. Parse sample metadata from the ISA-Tab files
3. Cache results locally for faster subsequent runs

### Optional: Pre-Download Cache

For faster subsequent runs, you can pre-download data for multiple studies:

```bash
# Download data for all demo studies
python -m cli.init_cache

# Or download specific studies only
python -m cli.init_cache --studies OSD-102 OSD-242
```

This will create:
- `outputs/enriched_csv/my_data_enriched.csv`
- `outputs/provenance_logs/my_data_provenance_YYYYMMDD_HHMMSS.json`

### With Validation Report

```bash
python -m cli.process_csv my_data.csv --validate
```

Adds: `outputs/validation_reports/my_data_validation_YYYYMMDD_HHMMSS.txt`

### Custom Output Path

```bash
python -m cli.process_csv input.csv -o outputs/enriched_csv/custom_output.csv
```

---

## 3. CLI Reference

### Main Command: `process_csv`

```bash
python -m cli.process_csv <input_csv> [OPTIONS]
```

### Required Arguments

| Argument | Description |
|----------|-------------|
| `input_csv` | Path to input CSV file |

### Optional Arguments

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Output CSV path | `outputs/enriched_csv/<input>_enriched.csv` |
| `--provenance` | Provenance log path | `outputs/provenance_logs/<input>_provenance.json` |
| `--validation-report` | Validation report path | Auto-generated if `--validate` |
| `--validate` | Generate validation report | `False` |
| `--no-cache` | Bypass local cache, fetch fresh | `False` |
| `--clear-cache` | Clear cache before processing | `False` |
| `--no-isa-tab` | Skip ISA-Tab download | `False` |
| `--osd-column` | Name of OSD ID column | `osd_id` |
| `--sample-column` | Name of sample ID column | `sample_id` |
| `-v, --verbose` | Enable verbose output | `False` |
| `-q, --quiet` | Suppress non-error output | `False` |

### Examples

```bash
# Standard enrichment with validation
python -m cli.process_csv data.csv --validate

# Fresh fetch (bypass cache)
python -m cli.process_csv data.csv --no-cache

# Clear cache and re-fetch all
python -m cli.process_csv data.csv --clear-cache

# Specify custom column names
python -m cli.process_csv data.csv --osd-column "Study ID" --sample-column "Sample Name"

# Full verbose output
python -m cli.process_csv data.csv --validate -v

# Quiet mode (errors only)
python -m cli.process_csv data.csv -q
```

### Single Study Processing

Process a single OSDR study:

```bash
python -m cli.process_osd_study OSD-242 --samples --factors
```

---

## 4. Input File Formats

### Supported Formats

- CSV (comma-separated)
- TSV (tab-separated)
- Excel (.xlsx, .xls)

### Required Columns

Your input CSV must contain at minimum:

| Column | Description | Example Values |
|--------|-------------|----------------|
| `osd_id` (or aliases) | OSDR study identifier | `OSD-242`, `242`, `OSD242` |
| `sample_id` (or aliases) | Sample identifier | `Mmus_C57-6J_BRN_HLU_7d_Rep1` |

### Recognized Column Aliases

The flexible loader automatically recognizes these column name variations:

| Canonical Name | Recognized Aliases |
|----------------|-------------------|
| `osd_id` | `OSD`, `OSD_study`, `study_id`, `accession` |
| `sample_id` | `mouse_uid`, `Sample Name`, `sample_name`, `source_name` |
| `mouse_strain` | `strain`, `Strain` |
| `mouse_sex` | `sex`, `Sex` |

### Example Input CSV

```csv
osd_id,sample_id,strain,mouse_sex,organ_sampled
OSD-242,Mmus_C57-6J_BRN_HLU_7d_Rep1,,,Brain
OSD-242,Mmus_C57-6J_BRN_HLU_7d_Rep2,,,Brain
OSD-102,Sample_FLT_Liver_M1,,,
```

### Minimal Input

```csv
osd_id,sample_id
OSD-242,Sample_001
OSD-242,Sample_002
```

The engine will enrich all other fields from NASA APIs.

---

## 5. Output Files

### Enriched CSV

**Location:** `outputs/enriched_csv/<name>_enriched.csv`

Contains all original columns plus enriched values:

| Column | Description | Source |
|--------|-------------|--------|
| `mouse_strain` | Normalized strain | ISA characteristics |
| `mouse_sex` | `Male` / `Female` / `Mixed` | ISA characteristics |
| `age` | Age with units | ISA characteristics/factors |
| `mouse_id` | Subject identifier | Tiered inference |
| `organ_sampled` | Tissue type | ISA characteristics |
| `space_or_ground` | `space` / `ground` / `HLU` | Factor values |
| `when_was_the_sample_collected` | Timepoint | Factor values / sample ID |
| `assay_on_organ` | Technology type | Study metadata |

### Provenance Log

**Location:** `outputs/provenance_logs/<name>_provenance_YYYYMMDD_HHMMSS.json`

Structure:
```json
{
  "metadata": {
    "generated_at": "2025-12-09T22:12:07",
    "total_entries": 14025,
    "total_conflicts": 0
  },
  "provenance": {
    "OSD-242": {
      "sample_001": {
        "mouse_strain": {
          "value": "C57BL/6J",
          "source": "from_isa_characteristics",
          "confidence": "high",
          "evidence_path": "characteristics[].Strain",
          "original_value": null,
          "inference_rule": null,
          "timestamp": "2025-12-09T22:12:07"
        }
      }
    }
  },
  "summary": { ... },
  "confidence_stats": { ... }
}
```

### Validation Report

**Location:** `outputs/validation_reports/<name>_validation_YYYYMMDD_HHMMSS.txt`

Human-readable report containing:
- Pipeline summary
- Provenance breakdown by field and source
- Confidence statistics
- Detected conflicts
- Errors and warnings

---

## 6. Understanding Provenance

### Provenance Sources

Every enriched value is tagged with its source:

| Source | Code | Meaning |
|--------|------|---------|
| OSDR API | `from_osdr_metadata` | Direct from Biodata/Developer API |
| ISA Characteristics | `from_isa_characteristics` | From ISA-Tab `characteristics[]` |
| ISA Factor Values | `from_isa_factor_values` | From ISA-Tab `factorValues[]` |
| Sample Name | `inferred_from_sample_name_structure` | Pattern-matched from ID |
| Not Applicable | `not_applicable` | Field doesn't apply (e.g., cell lines) |

### Confidence Levels

| Level | Meaning | When Used |
|-------|---------|-----------|
| `high` | Direct extraction | Structured API/ISA fields |
| `medium` | Pattern inference | Sample ID parsing |
| `low` | Grouping inference | Replicate prefix only |
| `n/a` | Not applicable | Non-rodent studies |

### Inference Rules

Pattern-based enrichments include the specific rule:

| Rule | Pattern | Example |
|------|---------|---------|
| `tier1_source_name` | Explicit source_name | `Mouse_02` |
| `tier2_m_pattern` | `_M{N}` in sample ID | `_M2` → `2` |
| `sample_id_days` | `_Nd_` or `_Nday_` | `_7d_` → `7 days` |
| `sample_id_months` | `_Nmonths` | `_7.5months` → `7.5 months` |

---

## 7. Validation Reports

### Reading the Report

```
NASA OSDR METADATA ENRICHMENT - VALIDATION REPORT
Generated: 2025-12-09 22:12:07
============================================================

PIPELINE SUMMARY
----------------------------------------
Total rows processed: 2712
Rows enriched: 2712
Studies processed: 27
Duration: 45.23 seconds

PROVENANCE SUMMARY
----------------------------------------
mouse_strain: 2712 enrichments
  - from_isa_characteristics: 2307
  - not_applicable: 405

CONFIDENCE BREAKDOWN
----------------------------------------
high: 11971
medium: 1649
low: 0
n/a: 405

CONFLICTS DETECTED: 0
```

### Interpreting Results

- **High enrichment rate** = Good API/ISA coverage
- **Medium confidence entries** = Review sample ID parses
- **Conflicts** = Sources disagree; review manually
- **N/A entries** = Non-rodent studies correctly handled

---

## 8. Common Workflows

### Workflow 1: New Study Batch

Processing a new batch of assigned studies:

```bash
# 1. Prepare input CSV with OSD IDs and sample IDs
# 2. Run enrichment with fresh fetch
python -m cli.process_csv new_studies.csv --no-cache --validate

# 3. Review validation report
cat outputs/validation_reports/new_studies_validation_*.txt

# 4. Check conflicts (if any)
cat outputs/validation_reports/new_studies_conflicts.txt
```

### Workflow 2: Re-enrichment After Updates

When OSDR data has been updated:

```bash
# Clear cache and re-fetch
python -m cli.process_csv studies.csv --clear-cache --validate
```

### Workflow 3: Quick Iteration

For rapid testing without network calls:

```bash
# Use cached data only
python -m cli.process_csv studies.csv
```

### Workflow 4: ML Dataset Preparation

Generate clean datasets for machine learning:

```bash
# Full enrichment with all validation
python -m cli.process_csv ml_training_data.csv \
    -o outputs/ml_json/training_data.csv \
    --validate

# Review confidence distribution in provenance
python -c "
import json
with open('outputs/provenance_logs/ml_training_data_provenance.json') as f:
    data = json.load(f)
    print(data['confidence_stats'])
"
```

---

## 9. Troubleshooting

### Common Issues

#### "No metadata available for OSD-XXX"

**Cause:** Study not found in OSDR APIs or private.

**Solution:**
- Verify OSD ID is correct
- Check if study is published on [osdr.nasa.gov](https://osdr.nasa.gov)
- Some studies require ISA-Tab fallback (ensure `--no-isa-tab` is not set)

#### "Column not found: osd_id"

**Cause:** Input CSV uses non-standard column names.

**Solution:**
```bash
python -m cli.process_csv data.csv --osd-column "Study ID" --sample-column "Sample"
```

#### High number of "medium confidence" entries

**Cause:** Timepoints extracted from sample IDs rather than factor values.

**Solution:** This is expected for some studies. Review the sample ID patterns in the provenance log to verify correctness.

#### Conflicts between API and ISA-Tab

**Cause:** Data discrepancy between sources.

**Solution:**
- Review `_conflicts.txt` report
- Check original study on OSDR website
- Manually verify correct value

### Debug Mode

For detailed debugging:

```bash
python -m cli.process_csv data.csv -v 2>&1 | tee debug.log
```

---

## 10. Advanced Usage

### Custom Output Directory Structure

```bash
python -m cli.process_csv input.csv \
    -o my_project/enriched/data.csv \
    --provenance my_project/logs/provenance.json \
    --validation-report my_project/reports/validation.txt
```

### Programmatic Usage

```python
from pathlib import Path
from src.core.pipeline import Pipeline, PipelineConfig

config = PipelineConfig(
    input_csv_path=Path("input.csv"),
    output_csv_path=Path("output_enriched.csv"),
    provenance_log_path=Path("provenance.json"),
    validation_report_path=Path("validation.txt"),
    use_cache=True,
    fetch_isa_tab=True,
)

pipeline = Pipeline(config)
result = pipeline.run()

print(f"Enriched {result.enriched_rows}/{result.total_rows} rows")
print(f"Conflicts: {result.conflict_count}")
```

### Processing Multiple Files

```bash
for f in resources/test_inputs/*.csv; do
    python -m cli.process_csv "$f" --validate
done
```

### Batch Processing Script

```python
#!/usr/bin/env python3
"""Batch process multiple CSV files."""

import subprocess
from pathlib import Path

input_dir = Path("resources/test_inputs")
output_dir = Path("outputs/batch")
output_dir.mkdir(exist_ok=True)

for csv_file in input_dir.glob("*.csv"):
    output_file = output_dir / f"{csv_file.stem}_enriched.csv"
    subprocess.run([
        "python", "-m", "cli.process_csv",
        str(csv_file),
        "-o", str(output_file),
        "--validate"
    ])
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OSDR_CACHE_DIR` | Custom cache directory | `resources/osdr_api/raw` |
| `OSDR_ISA_DIR` | Custom ISA-Tab directory | `resources/isa_tab` |

---

## API Rate Limiting

The pipeline includes built-in caching to minimize API load. For bulk operations:

- **First run:** Allow ~2-5 seconds per study for API calls
- **Subsequent runs:** Near-instant from cache
- **Fresh fetch:** Use `--no-cache` sparingly

---

## Getting Help

```bash
# Show CLI help
python -m cli.process_csv --help

# Show this documentation
cat USAGE.md
```

---

<div align="center">

**Need more help?** Open an issue in the repository.

</div>

