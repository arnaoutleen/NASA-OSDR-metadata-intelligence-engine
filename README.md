# NASA OSDR Metadata Intelligence Engine

**A science-grade metadata enrichment pipeline for NASA's Open Science Data Repository**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> **Author:** Yeshasvi Kamma  
> **Version:** 1.0.0

---

## Overview

The OSDR Metadata Intelligence Engine automates the extraction, normalization, and validation of multi-omics study metadata from NASA's Open Science Data Repository. It transforms fragmented, heterogeneous metadata into clean, consistent, fully traceable datasets—ready for meta-analysis and machine learning applications.

### Key Features

- **Zero-hallucination enrichment** — Only fills values from verified NASA data sources
- **Full provenance tracking** — Every enriched value is traceable to its origin
- **Organism-aware processing** — Correctly handles rodent vs. cell line studies
- **FAIR-compliant outputs** — Standardized CSVs with complete audit trails
- **Automatic data fetching** — Downloads and parses ISA-Tab on demand
- **Deterministic & reproducible** — Same input always produces same output

### The Problem

Space biology researchers face significant barriers when working with OSDR metadata:

| Challenge | Impact |
|-----------|--------|
| **Inconsistent terminology** | "Flight" vs. "Space Flight" vs. "Spaceflight" |
| **Missing sample attributes** | Strain, sex, timepoints buried in free text |
| **Complex ISA-Tab hierarchy** | Nested Investigation → Study → Assay structure |
| **Manual curation burden** | Hours spent before analysis can begin |

### The Solution

This pipeline provides automated, ethical metadata enrichment:

```
Input CSV → API/ISA-Tab Retrieval → Enrichment → Validation → Enriched CSV + Provenance Log
```

---

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/yeshasvikamma/NASA-OSDR-metadata-intelligence-engine.git
cd NASA-OSDR-metadata-intelligence-engine
pip install -r requirements.txt

# 2. Run enrichment (data is fetched automatically)
python -m cli.process_csv resources/test_inputs/demo/realworld_rodent_research.csv --validate

# 3. Review outputs
ls outputs/enriched_csv/       # Enriched CSV
ls outputs/provenance_logs/    # Provenance JSON
ls outputs/validation_reports/ # Validation report
```

📖 **See [USAGE.md](USAGE.md) for complete documentation.**

---

## Architecture

```
nasa-osdr-metadata-engine/
├── cli/                          # Command-line interface
│   ├── process_csv.py            # Main enrichment CLI
│   ├── process_osd_study.py      # Single-study processing
│   └── init_cache.py             # Optional pre-fetching
├── src/
│   ├── core/                     # Core enrichment engine
│   │   ├── pipeline.py           # Pipeline orchestrator
│   │   ├── osdr_client.py        # NASA API client + ISA-Tab fetcher
│   │   ├── isa_parser.py         # ISA-Tab metadata parser
│   │   ├── enrichment_rules.py   # Enrichment logic
│   │   ├── provenance.py         # Audit trail system
│   │   └── constants.py          # Controlled vocabularies
│   ├── intelligence/             # Pattern recognition
│   │   ├── pattern_extractor.py  # Sample ID parsing
│   │   ├── biological_rules.py   # Organism-specific logic
│   │   └── unit_inference.py     # Time/dose extraction
│   ├── validation/               # Quality control
│   │   ├── conflict_checker.py   # Cross-source conflicts
│   │   └── schema_validator.py   # Schema validation
│   └── utils/                    # Shared utilities
│       ├── flexible_loader.py    # Flexible CSV loading
│       └── config.py             # Configuration
├── resources/
│   ├── test_inputs/demo/         # Demo test files
│   └── schema/                   # Column schemas
└── outputs/                      # Generated outputs
    ├── enriched_csv/
    ├── provenance_logs/
    └── validation_reports/
```

---

## Data Sources

The pipeline retrieves metadata from official NASA endpoints with intelligent fallback:

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | **Biodata API** | `visualization.osdr.nasa.gov/biodata/api/v2/` |
| 2 | **Developer API** | `osdr.nasa.gov/osdr/data/` |
| 3 | **ISA-Tab Files** | `genelab-data.ndc.nasa.gov/` (automatically downloaded) |

All successful retrievals are cached locally to minimize API load.

---

## Provenance System

Every enriched value includes complete provenance:

```json
{
  "OSD-242": {
    "sample_001": {
      "mouse_strain": {
        "value": "C57BL/6J",
        "source": "from_isa_characteristics",
        "confidence": "high",
        "evidence_path": "characteristics[].Strain",
        "timestamp": "2025-12-09T22:12:07"
      }
    }
  }
}
```

### Confidence Levels

| Level | Meaning | Example |
|-------|---------|---------|
| **high** | Direct extraction from structured fields | Strain from ISA characteristics |
| **medium** | Pattern-based inference | Timepoint from sample ID |
| **low** | Grouping-based inference | Replicate prefix matching |
| **n/a** | Explicitly not applicable | Mouse fields for cell lines |

---

## Controlled Vocabularies

The engine normalizes values to canonical forms:

| Category | Normalization Examples |
|----------|------------------------|
| **Strains** | `c57bl/6j` → `C57BL/6J` · `balb/c` → `BALB/c` |
| **Sex** | `M` / `male` → `Male` · `F` / `female` → `Female` |
| **Groups** | `Flight` / `FLT` → `space` · `Ground Control` / `GC` → `ground` |
| **Tissues** | Standard anatomical nomenclature with abbreviation support |

---

## Validation

The pipeline performs non-blocking validation checks:

- **Cross-source conflict detection** — API vs. ISA-Tab discrepancies
- **Biological consistency checks** — Strain-organism matching, age plausibility
- **Completeness statistics** — Per-field fill rates with organism context
- **Confidence breakdown** — Distribution of enrichment reliability

---

## FAIR Data Compliance

| Principle | Implementation |
|-----------|----------------|
| **Findable** | OSD identifiers link to source studies |
| **Accessible** | All data from public NASA APIs |
| **Interoperable** | Standardized schemas, normalized terminology |
| **Reusable** | Full provenance enables reproducibility |

---

## Requirements

- Python 3.11+ (3.12 recommended)
- `requests>=2.28.0`
- `pandas>=1.5.0`

```bash
pip install -r requirements.txt
```

---

## Citation

When using outputs from this pipeline in publications:

1. **Cite original OSDR studies** using their accession numbers (OSD-XXX)
2. **Acknowledge NASA GeneLab** as the data source
3. **Reference this pipeline** with version and retrieval date

### Suggested Citation

> Metadata were compiled from NASA's Open Science Data Repository (OSDR) using the OSDR Metadata Intelligence Engine v1.0 (Kamma, Y., December 2025). Original study data available at https://osdr.nasa.gov/.

---

## License

MIT License. See [LICENSE](LICENSE) for details.

Users are responsible for complying with NASA's data use policies and appropriately citing source datasets.

---

## Contributing

This is a solo project by Yeshasvi Kamma. For questions, issues, or collaboration inquiries, please open an issue in this repository.

---

<div align="center">

**Built for NASA Space Biology Research**

*Enabling reproducible, traceable metadata for the space biology community*

</div>
