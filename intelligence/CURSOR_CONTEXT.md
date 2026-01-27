# NASA OSDR Metadata Intelligence Engine - Cursor Context

## Project Overview

This project is a **science-grade metadata enrichment engine** for NASA's Open Science Data Repository (OSDR) multi-omics studies. It transforms messy, fragmented OSDR metadata into clean, consistent, fully traceable metadata tables with strict biological correctness and zero hallucination.

## Core Design Principles

1. **Never overwrite** existing CSV data
2. **Never hallucinate** or guess values  
3. **Only fill blanks** when NASA data contains supporting evidence
4. **Provenance required** for every enriched value
5. **Deterministic and reproducible** - same input always produces same output

## Architecture Overview

```
NASA_discovery/
├── src/
│   ├── core/                    # Core enrichment engine
│   │   ├── __init__.py
│   │   ├── constants.py         # Controlled vocabularies (strains, tissues, assays)
│   │   ├── provenance.py        # ProvenanceEntry, ConflictEntry tracking
│   │   ├── osdr_client.py       # OSDR API client + ISA-Tab download
│   │   ├── isa_parser.py        # ISA-Tab Study/Assay file parsing
│   │   ├── sample_expander.py   # OSD → sample-level expansion (NEW)
│   │   ├── enrichment_rules.py  # Deterministic enrichment logic
│   │   └── pipeline.py          # Main pipeline orchestrator
│   ├── intelligence/            # AI/reasoning layer (suggestions only)
│   │   ├── pattern_extractor.py # Sample name pattern inference
│   │   ├── unit_inference.py    # Age/time unit detection
│   │   ├── biological_rules.py  # Biological consistency checks
│   │   └── ai_reasoner.py       # BioLLM interface (future)
│   ├── validation/              # Validation and conflict detection
│   │   ├── schema_validator.py  # Controlled vocab enforcement
│   │   ├── conflict_checker.py  # Cross-source discrepancy detection
│   │   ├── replicate_checker.py # Replicate pattern validation
│   │   └── timeline_reconstructor.py
│   ├── utils/                   # Shared utilities
│   │   ├── config.py            # Path and API configuration
│   │   ├── logging_utils.py     # Structured logging
│   │   └── file_utils.py        # CSV/JSON I/O helpers
│   ├── ml_builder/              # ML-ready exports (future)
│   └── dashboard/               # Visualization (future)
├── cli/                         # Command-line interface
│   ├── expand_samples.py        # OSD → sample expansion CLI (NEW)
│   ├── process_csv.py           # Main CSV enrichment CLI
│   ├── init_cache.py            # Pre-download cache for studies
│   └── process_osd_study.py     # Single study processing
├── resources/                   # Raw data (preserved)
│   ├── osdr_api/raw/            # Cached OSDR JSON
│   ├── isa_tab/                 # Downloaded ISA-Tab bundles
│   ├── input_study_summary/     # Input OSD lists for expansion
│   ├── example_study_summary/   # Example expanded outputs
│   ├── test_inputs/demo/        # Demo test files
│   └── schema/                  # Column schemas
├── outputs/                     # Pipeline outputs
│   ├── enriched_csv/
│   ├── provenance_logs/
│   ├── validation_reports/
│   └── ml_json/
├── intelligence/                # Project context docs
│   └── CURSOR_CONTEXT.md        # THIS FILE
├── tests/
├── README.md
└── requirements.txt
```

## Key Data Sources

### OSDR API Endpoints (Priority Order)

1. **Biodata API (Primary)**: `https://visualization.osdr.nasa.gov/biodata/api/v2/`
   - `/dataset/{OSD-ID}/` - Dataset-level metadata
   - `/dataset/{OSD-ID}/assay/*/sample/*/` - All samples with characteristics
   
2. **Developer API (Fallback)**: `https://osdr.nasa.gov/osdr/data/`
   - `/osd/meta/{numeric_id}` - Study-level metadata
   
3. **ISA-Tab Files (Last Resort)**
   - `s_*.txt` - Study file with sample characteristics
   - `a_*.txt` - Assay files

### ISA-Tab Structure

- **Investigation file** (`i_*.txt`): Project-level metadata
- **Study file** (`s_*.txt`): Sample characteristics, source-sample mappings
- **Assay file** (`a_*.txt`): Technology-specific sample info

## Provenance Tracking

Every enriched field MUST have provenance:

```python
ProvenanceEntry(
    osd_id="OSD-242",
    sample_id="Mmus_C57-6J_BRN_HLU_IR_7d_Rep1_M2",
    field_name="mouse_strain",
    value="C57BL/6NTac",
    source=ProvenanceSource.ISA_CHARACTERISTICS,  # from enum
    confidence=ConfidenceLevel.HIGH,
    evidence_path="characteristics[].Strain",
    original_value="C57BL/6NTac",
    inference_rule=None
)
```

### Valid Provenance Sources

- `from_osdr_metadata` - From OSDR API JSON
- `from_isa_characteristics` - From ISA-Tab characteristics
- `from_isa_factor_values` - From ISA-Tab factor values
- `from_study_description` - Parsed from study description
- `inferred_from_sample_name_structure` - Pattern-matched from naming
- `not_applicable` - Field not applicable (e.g., mouse fields for cell lines)

### Confidence Levels

- `high` - Direct extraction from structured API/ISA-Tab fields
- `medium` - Pattern-based inference with clear conventions
- `low` - Grouping-based or uncertain inference
- `suggestion` - AI-generated (requires human review)
- `n/a` - Not applicable

## Enriched Fields

### Sample-Level Fields
- `mouse_strain` - From sample characteristics (Strain)
- `mouse_sex` - From sample characteristics (Sex)
- `age` - From sample characteristics (Age)
- `mouse_id` - Inferred from source_name or sample ID patterns
- `organ_sampled` - From sample characteristics (Material Type, Organism Part)
- `space_or_ground` - From factor values (Spaceflight, Group)
- `when_was_the_sample_collected` - From factor values or sample ID timepoint patterns

### Study-Level Fields
- `Has_RNAseq` - Detected from assay filenames
- `n_mice_total` - Count of unique source_names (animals)
- `n_RNAseq_mice` - Count of animals with RNA-seq data
- `mouse_genetic_variant` - From sample characteristics (Genotype)
- `mouse_source` - From sample characteristics (Animal Source)
- `time_in_space` - From factor values (Duration) or description parsing
- `study purpose` - From study description field
- `age_when_sent_to_space` - From sample characteristics (Age at Launch)
- `assay_on_organ` - From assay types metadata

## Sample Name Patterns

### Organism Prefixes
- `Mmus_` → Mus musculus
- `Rnor_` → Rattus norvegicus
- `Hsap_` → Homo sapiens

### Tissue Abbreviations
- `BRN` → Brain
- `HRT` → Heart
- `LVR` → Liver
- `SOL` → Soleus
- `GAS` → Gastrocnemius

### Condition Codes
- `FLT` → Flight (spaceflight)
- `GC` → Ground Control
- `HLU` → Hindlimb Unloading
- `IR` → Irradiated

### Timepoint Patterns
- `_7d_` → 7 days
- `_R7_` → 7 days post-return
- `_L14_` → 14 days pre-launch

### Mouse ID Patterns
- `_M2` → Mouse 2
- `_Rep1` → Replicate 1

## Common Tasks

### Expand OSD IDs to Sample Rows (NEW)

```bash
python -m cli.expand_samples resources/input_study_summary/study_list.csv \
    -o outputs/enriched_csv/expanded_samples.csv
```

### Run Full Enrichment Pipeline

```bash
python -m cli.process_csv resources/test_inputs/demo/realworld_rodent_research.csv \
    -o outputs/enriched_csv/enriched_output.csv --validate
```

### Process Single Study

```bash
python -m cli.process_osd_study OSD-242 --samples --factors
```

### Clear Cache and Re-fetch

```bash
python -m cli.process_csv input.csv --clear-cache
```

## Key Studies for Testing

| OSD ID | Type | Key Features |
|--------|------|--------------|
| OSD-202 | Ground (HLU + Radiation) | 4 timepoints, multiple groups |
| OSD-242 | Ground (HLU + Radiation) | Rich characteristics, brain tissue |
| OSD-546 | Spaceflight (Soyuz) | ~170 days mission, stem cells |
| OSD-661 | Ground (HLU + Genotype) | KD vs Flox inference |
| OSD-102 | Spaceflight (RR-1) | Mouse IDs in sample names |

## Code Organization Rules

1. **Type hints** on all function parameters and returns
2. **Docstrings** for all public functions and classes
3. **Imports** from `src.core`, `src.intelligence`, etc.
4. **No external dependencies** beyond `requests` and `pandas`
5. **Test coverage** for all enrichment rules

## What Cursor Should Output

When asked for code:
- Clean, functional Python
- Full type hints
- Docstrings for public APIs
- No unnecessary explanations
- Follow existing patterns in codebase

## Sample Expander Module (NEW)

The `src/core/sample_expander.py` module provides OSD → sample-level expansion:

### Key Classes

- **SampleRow**: Dataclass representing one output row
- **SampleExpander**: Main class for parsing ISA-Tab and generating sample rows

### Key Functions

- `expand_osd_to_samples(osd_id, rr_mission)` - Expand one OSD to samples
- `expand_multiple_osds(osd_ids)` - Batch expansion
- `expand_samples_from_csv(input_path, output_path)` - Full CSV workflow

### Output Columns

The expander generates these columns:
- `RR_mission` - Mission name (RR-1, MHU-3, Hindlimb_Unloading, etc.)
- `OSD_study` - OSD identifier
- `mouse_uid` - Animal/subject ID (from source_name)
- `sample_name` - Sample identifier
- `extract_name` - Extract identifier
- `space_or_ground` - `spaceflight` or `ground`
- `when_was_the_sample_collected` - Collection timepoint
- `mouse_sex` - Male/Female
- `mouse_strain` - Strain name
- `mouse_genetic_variant` - Genotype (Wild, KO, Het, etc.)
- `mouse_source` - Vendor/source
- `organ_sampled` - Tissue type
- `assay_on_organ` - Assay type (rna-seq, proteomics, etc.)
- `RNA_seq_method` - Library selection method
- `RNA_seq_paired` - PAIRED or SINGLE

---

*Last updated: January 2026*
*For the full pipeline documentation, see README.md*
