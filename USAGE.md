# USAGE — Detailed Reference Manual

NASA OSDR Metadata Intelligence Engine v2.0.0

---

## 1. Installation

```bash
git clone https://github.com/yeshasvikamma/NASA-OSDR-metadata-intelligence-engine.git
cd NASA-OSDR-metadata-intelligence-engine
python -m venv venv
source venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
```

Verify the installation:

```bash
python -c "from src.core.data_retriever import DataRetriever; print('OK')"
```

**Network requirements:** The first run for any OSD study requires internet access to NASA OSDR APIs. Subsequent runs use locally cached data.

---

## 2. CLI Reference

### 2.1 `cli/retrieve_data.py` — Data Retrieval

Fetch unified sample records from NASA OSDR.

```
python -m cli.retrieve_data [OSD_IDS ...] [--mission NAME] [--all] [-o PATH] [--format csv|json]
```

| Flag | Description | Default | Required |
|---|---|---|---|
| `OSD_IDS` | One or more OSD study IDs | — | One of OSD_IDS, --mission, or --all |
| `--mission` | Retrieve all OSDs for a mission (e.g., `RR-3`) | — | |
| `--all` | Retrieve all known studies | `false` | |
| `-o`, `--output` | Output file path | `outputs/retrieval/<label>_data.<ext>` | No |
| `--format` | Output format: `csv` or `json` | `csv` | No |
| `--no-cache` | Skip local cache, fetch fresh | `false` | No |
| `--clear-cache` | Clear all cached metadata first | `false` | No |
| `-v`, `--verbose` | Verbose output | `false` | No |
| `-q`, `--quiet` | Suppress all output except errors | `false` | No |

**Examples:**

```bash
# Retrieve specific studies
python -m cli.retrieve_data OSD-242 OSD-379 OSD-102

# Retrieve all studies in a mission
python -m cli.retrieve_data --mission RR-3 -o outputs/retrieval/rr3_data.csv

# Retrieve in JSON format
python -m cli.retrieve_data OSD-242 --format json -o outputs/retrieval/osd242.json
```

**Output columns (CSV):** `osd`, `source_name`, `sample_name`, `organism`, `material_type`, `measurement_types`, `technology_types`, `device_platforms`, `data_files`

List fields (`measurement_types`, `technology_types`, `device_platforms`, `data_files`) are pipe-separated in CSV and arrays in JSON.

---

### 2.2 `cli/rank_samples.py` — Sample Informativeness (Table 1)

Rank samples by data availability within each OSD.

```
python -m cli.rank_samples [OSD_IDS ...] [--mission NAME] [-o PATH] [--format csv|json]
```

| Flag | Description | Default | Required |
|---|---|---|---|
| `OSD_IDS` | One or more OSD study IDs | — | One of OSD_IDS or --mission |
| `--mission` | Rank samples for all OSDs in a mission | — | |
| `-o`, `--output` | Output file path | `outputs/rankings/sample_ranking_<label>.<ext>` | No |
| `--format` | Output format: `csv` or `json` | `csv` | No |
| `-v`, `--verbose` | Verbose output | `false` | No |
| `-q`, `--quiet` | Suppress output except errors | `false` | No |

**Examples:**

```bash
# Single OSD
python -m cli.rank_samples OSD-242 -o outputs/rankings/osd242_samples.csv

# Entire mission
python -m cli.rank_samples --mission RR-3

# Multiple OSDs
python -m cli.rank_samples OSD-102 OSD-242 OSD-379
```

**Output columns:** `OSD`, `source_name`, `sample_name`, `organ`, `assay_types`, `num_assays`, `data_files_count`, `measurement_types`, `technology_types`, `device_platforms`, `informativeness_rank`

Sorted by `num_assays` descending, then `data_files_count` descending. `informativeness_rank` is 1 for the most data-rich tier within each OSD.

---

### 2.3 `cli/rank_mice.py` — Mouse Informativeness (Table 2)

Rank mice across all OSDs in a mission by cross-organ data coverage.

```
python -m cli.rank_mice --mission NAME [-o PATH] [--format csv|json]
```

| Flag | Description | Default | Required |
|---|---|---|---|
| `--mission` | Mission name (e.g., `RR-3`, `RR-1`) | — | **Yes** |
| `-o`, `--output` | Output file path | `outputs/rankings/mouse_ranking_<mission>.<ext>` | No |
| `--format` | Output format: `csv` or `json` | `csv` | No |
| `-v`, `--verbose` | Verbose output | `false` | No |
| `-q`, `--quiet` | Suppress output except errors | `false` | No |

**Examples:**

```bash
python -m cli.rank_mice --mission RR-3 -o outputs/rankings/rr3_mice.csv
python -m cli.rank_mice --mission RR-1 --format json
```

**Output columns:** `mission`, `source_name`, `osds`, `num_organs`, `organs_list`, `num_total_assays`, `assays_per_organ`, `total_data_files`, `informativeness_score`

Sorted by `informativeness_score` descending. The `assays_per_organ` column contains a JSON-encoded dictionary mapping each organ to its list of assay types.

---

### 2.4 `cli/rank_all.py` — Combined Ranking (Both Tables)

Generate both sample and mouse informativeness tables for a mission or all missions.

```
python -m cli.rank_all [--mission NAME] [--all] [-o DIR] [--format csv|json]
```

| Flag | Description | Default | Required |
|---|---|---|---|
| `--mission` | Mission name | — | One of --mission or --all |
| `--all` | Generate tables for all 12 known missions | `false` | |
| `-o`, `--output` | Output **directory** | `outputs/rankings/` | No |
| `--format` | Output format: `csv` or `json` | `csv` | No |
| `-v`, `--verbose` | Verbose output | `false` | No |
| `-q`, `--quiet` | Suppress output except errors | `false` | No |

**Examples:**

```bash
# Single mission → creates sample_ranking_RR-3.csv and mouse_ranking_RR-3.csv
python -m cli.rank_all --mission RR-3 -o outputs/rankings/

# All missions
python -m cli.rank_all --all -o outputs/rankings/
```

**Output files:** For each mission, two files are created in the output directory:
- `sample_ranking_<mission>.csv` — Table 1
- `mouse_ranking_<mission>.csv` — Table 2

---

### 2.5 `cli/expand_samples.py` — Sample Expansion

Expand a CSV of OSD IDs into sample-level rows with characteristics from ISA-Tab.

```
python -m cli.expand_samples INPUT_CSV [-o PATH] [--osd-column NAME] [--mission-column NAME] [--no-grouping]
```

| Flag | Description | Default | Required |
|---|---|---|---|
| `INPUT_CSV` | Path to input CSV containing OSD IDs | — | **Yes** |
| `-o`, `--output` | Output CSV path | `outputs/enriched_csv/<input>_samples.csv` | No |
| `--osd-column` | Column name containing OSD IDs | `OSD_study` | No |
| `--mission-column` | Column name containing mission names | `RR_mission` | No |
| `--no-grouping` | Show mission/OSD on every row | `false` | No |
| `-v`, `--verbose` | Verbose output | `false` | No |
| `-q`, `--quiet` | Suppress output except errors | `false` | No |

**Examples:**

```bash
python -m cli.expand_samples resources/test_inputs/demo/realworld_study_summary.csv --osd-column osd_id -o outputs/expanded.csv
```

**Output columns:** `RR_mission`, `OSD_study`, `mouse_uid`, `sample_name`, `extract_name`, `space_or_ground`, `when_was_the_sample_collected`, `mouse_sex`, `mouse_strain`, `mouse_genetic_variant`, `mouse_source`, `organ_sampled`, `assay_on_organ`, `number_of_tech_replicates`, `part_of_a_longitudinal_sample_series`, `notes`, `RNA_seq_method`, `RNA_seq_paired`, `days_in_space_rr3`

**Note:** If your input CSV uses `osd_id` as the column name (instead of the default `OSD_study`), pass `--osd-column osd_id`.

---

### 2.6 `cli/process_csv.py` — Metadata Enrichment

Run the metadata enrichment pipeline on a researcher-provided CSV.

```
python -m cli.process_csv INPUT_CSV [-o PATH] [--validate] [--no-cache] [--clear-cache]
```

| Flag | Description | Default | Required |
|---|---|---|---|
| `INPUT_CSV` | Path to input CSV file | — | **Yes** |
| `-o`, `--output` | Enriched CSV output path | `outputs/enriched_csv/<input>_enriched.csv` | No |
| `--provenance` | Provenance log JSON path | `outputs/provenance_logs/<input>_provenance_<ts>.json` | No |
| `--validation-report` | Validation report path | `outputs/validation_reports/<input>_validation_<ts>.txt` | No |
| `--no-cache` | Bypass cached metadata | `false` | No |
| `--clear-cache` | Clear all cached metadata first | `false` | No |
| `--no-isa-tab` | Skip ISA-Tab downloads (faster, less complete) | `false` | No |
| `--validate` | Generate a validation report | `false` | No |
| `--osd-column` | OSD ID column name | `osd_id` | No |
| `--sample-column` | Sample ID column name | `sample_id` | No |
| `-v`, `--verbose` | Verbose output | `false` | No |
| `-q`, `--quiet` | Suppress output except errors | `false` | No |

**Examples:**

```bash
# Enrich with validation report
python -m cli.process_csv resources/test_inputs/demo/realworld_rodent_research.csv --validate

# Specify output path
python -m cli.process_csv input.csv -o outputs/enriched_csv/enriched.csv

# Fresh fetch (no cache)
python -m cli.process_csv input.csv --no-cache
```

**Outputs produced:**
1. Enriched CSV — input rows with empty fields filled from OSDR/ISA-Tab data
2. Provenance JSON — structured log of every enrichment with source and confidence
3. Validation report (if `--validate`) — human-readable summary of enrichments and conflicts

The pipeline uses a flexible loader that accepts CSV, TSV, and JSON inputs. Column names are automatically mapped from common aliases (e.g., `mouse_strain` → `strain`).

---

### 2.7 `cli/process_osd_study.py` — Single Study Inspection

Fetch and display metadata for a single OSD study.

```
python -m cli.process_osd_study OSD_ID [--samples] [--factors] [--export-json PATH] [--download-isa-tab]
```

| Flag | Description | Default | Required |
|---|---|---|---|
| `OSD_ID` | OSD study identifier (e.g., `OSD-242` or `242`) | — | **Yes** |
| `--samples` | Show sample-level details | `false` | No |
| `--factors` | Show factor values for each sample | `false` | No |
| `--limit` | Max samples to display | `10` | No |
| `--export-json` | Export metadata to JSON file | — | No |
| `--download-isa-tab` | Download and extract ISA-Tab archive | `false` | No |
| `--no-cache` | Bypass cached metadata | `false` | No |
| `--clear-cache` | Clear cache for this study | `false` | No |

**Examples:**

```bash
python -m cli.process_osd_study OSD-242 --samples --factors
python -m cli.process_osd_study OSD-102 --export-json study_102.json --download-isa-tab
```

---

### 2.8 `cli/init_cache.py` — Cache Initialization

Pre-download ISA-Tab archives so the pipeline works offline.

```
python -m cli.init_cache [--studies OSD_IDS ...] [-q]
```

| Flag | Description | Default | Required |
|---|---|---|---|
| `--studies` | OSD IDs to cache | 8 demo studies (OSD-102, OSD-202, OSD-242, OSD-479, OSD-477, OSD-546, OSD-661, OSD-207) | No |
| `-q`, `--quiet` | Suppress output | `false` | No |

**Examples:**

```bash
# Cache default demo studies
python -m cli.init_cache

# Cache specific studies
python -m cli.init_cache --studies OSD-102 OSD-242 OSD-379
```

---

## 3. Input File Formats

### For `process_csv` (metadata enrichment)

The input CSV must contain at minimum an OSD ID column. The flexible loader recognizes these aliases:

| Canonical name | Accepted aliases |
|---|---|
| `osd_id` | `OSD_study`, `OSD`, `study_id`, `accession` |
| `sample_id` | `Sample Name`, `sample_name`, `mouse_uid`, `source_name` |
| `strain` | `mouse_strain` |

Additional columns (e.g., `mouse_sex`, `organ_sampled`, `age`, `space_or_ground`) will be enriched if empty.

### For `expand_samples`

A CSV with at least one column containing OSD IDs. The default column name is `OSD_study`. Use `--osd-column` if your column has a different name.

### For `retrieve_data`, `rank_samples`, `rank_mice`, `rank_all`

No input file needed — OSD IDs and mission names are passed as command-line arguments.

---

## 4. Output Files

### Retrieved Data (`retrieve_data`)

CSV with one row per sample. List fields are pipe-separated.

```csv
osd,source_name,sample_name,organism,material_type,measurement_types,technology_types,device_platforms,data_files
OSD-242,B1,Mmus_C57-6J_LVR_BSL_C1_Rep1_B1,Mus musculus,Liver,RNA-Seq,Rna Sequencing (Rna Seq),Illumina NovaSeq 6000,GLDS-242_rna-seq_..._R1_raw.fastq.gz | GLDS-242_rna-seq_..._R2_raw.fastq.gz
```

### Sample Ranking (`rank_samples`)

CSV with one row per sample, ranked by `num_assays` and `data_files_count`.

```csv
OSD,source_name,sample_name,organ,assay_types,num_assays,data_files_count,measurement_types,technology_types,device_platforms,informativeness_rank
OSD-102,RR1_FLT_M23,Mmus_C57-6J_KDN_FLT_Rep1_M23,Left kidney,Dna Methylation Profiling | ... | RNA-Seq,4,3,...,1
```

### Mouse Ranking (`rank_mice`)

CSV with one row per animal, ranked by `informativeness_score`.

```csv
mission,source_name,osds,num_organs,organs_list,num_total_assays,assays_per_organ,total_data_files,informativeness_score
RR-3,RR3_BSL_B7,OSD-137 | OSD-162 | OSD-194,3,Eye | Left retina | Liver,4,"{""Eye"": [""RNA-Seq""], ...}",5,14.58
```

### Enriched CSV (`process_csv`)

The original CSV with empty fields filled. New columns may be added (e.g., `Has_RNAseq`, `n_mice_total`, `study purpose`).

### Provenance JSON (`process_csv`)

Structured log with per-study, per-sample, per-field entries:

```json
{
  "metadata": {
    "generated_at": "2026-03-02T16:45:02",
    "total_entries": 168,
    "total_conflicts": 0
  },
  "provenance": {
    "OSD-102": {
      "Mmus_C57-6J_KDN_FLT_Rep1_M23": {
        "mouse_strain": {
          "value": "C57BL/6J",
          "source": "from_isa_characteristics",
          "confidence": "high"
        }
      }
    }
  },
  "summary": { ... },
  "confidence_stats": { "high": 154, "medium": 14, "low": 0 }
}
```

### Expanded CSV (`expand_samples`)

One row per sample with 19 columns describing the animal, organ, assay, and sequencing details.

---

## 5. Common Workflows

### Workflow 1: "What data is available for my mission?"

```bash
python -m cli.retrieve_data --mission RR-1 -o outputs/retrieval/rr1_data.csv
python -m cli.rank_all --mission RR-1 -o outputs/rankings/
```

This fetches all sample records across RR-1's 10 OSDs, then generates both the sample ranking table and the mouse ranking table.

### Workflow 2: "Which mice in RR-3 have the most multi-organ data?"

```bash
python -m cli.rank_mice --mission RR-3 -o outputs/rankings/rr3_mice.csv
```

Open the CSV and look at the top rows. Mice with `num_organs >= 3` and high `informativeness_score` are the best candidates for integrative multi-omics analysis.

### Workflow 3: "I have a list of OSD IDs and need enriched metadata"

```bash
python -m cli.process_csv my_studies.csv --validate -o outputs/enriched_csv/my_studies_enriched.csv
```

The pipeline fills empty fields (strain, sex, age, organ, assay type, etc.) from OSDR APIs and ISA-Tab, then writes a provenance log showing where every value came from.

### Workflow 4: "Expand a study list into sample-level rows"

```bash
python -m cli.expand_samples my_study_list.csv --osd-column osd_id -o outputs/expanded.csv
```

Each OSD ID becomes multiple rows — one per sample — with characteristics extracted from ISA-Tab.

### Workflow 5: "Build a complete dataset for all known missions"

```bash
python -m cli.rank_all --all -o outputs/rankings/
```

This iterates over all 12 known missions and generates `sample_ranking_<mission>.csv` and `mouse_ranking_<mission>.csv` for each one.

---

## 6. Understanding Provenance

Every value filled by the enrichment pipeline has a provenance source:

| Source code | Meaning | Confidence |
|---|---|---|
| `from_osdr_metadata` | Direct extraction from OSDR API JSON | High |
| `from_isa_characteristics` | From ISA-Tab `Characteristics[...]` column | High |
| `from_isa_factor_values` | From ISA-Tab `Factor Value[...]` column | High |
| `from_isa_study_file` | From ISA-Tab Study file (s_*.txt) | High |
| `from_isa_assay_file` | From ISA-Tab Assay file (a_*.txt) | High |
| `from_study_description` | Parsed from study description text | Medium |
| `from_mission_metadata` | From mission-level metadata | High |
| `inferred_from_sample_name_structure` | Pattern matched from sample name (e.g., `_M23` → mouse 23) | Medium |
| `inferred_from_cross_sample_linking` | Derived by linking across samples | Medium |
| `inferred_from_biological_rule` | Applied biological knowledge rule | Medium |
| `not_applicable` | Field doesn't apply (e.g., mouse strain for cell line study) | N/A |

### Confidence levels

| Level | Meaning |
|---|---|
| `high` | Direct extraction from a structured field |
| `medium` | Pattern-based inference with clear conventions |
| `low` | Grouping-based or uncertain inference |
| `suggestion` | AI-generated suggestion requiring human review |
| `n/a` | Not applicable to this study type |

---

## 7. Understanding Informativeness Scores

### Formula

```
score = num_organs × num_distinct_assay_types + log₂(1 + total_data_files)
```

### Why this formula

- **`num_organs × num_assays`** — a multiplicative term that rewards breadth. A mouse with 3 organs and 4 assay types scores 12, while a mouse with 1 organ and 4 assay types scores 4. This captures the value of multi-organ, multi-omics data.
- **`log₂(1 + total_data_files)`** — a logarithmic term that provides a small bonus for data volume. A mouse with 5 files gets +2.58, while one with 100 files gets +6.66. The log prevents file count from dominating the score.

### Worked example

Mouse `RR3_BSL_B7` in mission RR-3:

```
Organs:   Eye, Left retina, Liver         → num_organs = 3
Assays:   DNA Methylation, Imaging,
          Proteomics, RNA-Seq              → num_distinct_assay_types = 4
Files:    5 total                          → log₂(6) = 2.58

Score = 3 × 4 + 2.58 = 14.58
```

### When to use which table

- **Table 1 (Sample Ranking):** "Which samples in this OSD have the most assay types?" Use when deciding which samples to prioritize within a single study.
- **Table 2 (Mouse Ranking):** "Which animals across this mission have the most multi-organ coverage?" Use when planning integrative analyses that require data from multiple organs.

---

## 8. Mission Resolution

### How missions map to OSDs

The `MissionResolver` uses a three-tier strategy:

1. **Seed registry** — 12 manually curated missions in `src/core/constants.py` (`KNOWN_MISSIONS`)
2. **Cache** — persistent `mission_registry.json` in the cache directory
3. **Dynamic discovery** — parses mission names from ISA-Tab investigation titles and OSDR Developer API study descriptions

### Adding a new mission

To add a new mission to the seed registry, edit `KNOWN_MISSIONS` in `src/core/constants.py`:

```python
KNOWN_MISSIONS: Dict[str, Tuple[str, ...]] = {
    ...
    "RR-99": ("OSD-999", "OSD-1000"),
}
```

Add aliases in `MISSION_ALIASES`:

```python
MISSION_ALIASES: Dict[str, str] = {
    ...
    "rr99": "RR-99", "rr-99": "RR-99",
}
```

The resolver also discovers missions dynamically: if a study's ISA-Tab title contains "Rodent Research 99", it will be automatically linked.

---

## 9. Troubleshooting

### "No OSDs found for mission X"

The mission name may not be in the seed registry or cache. Check:
1. Spelling — use the canonical form (e.g., `RR-3`, not `RR3` or `Rodent Research-3`)
2. `python -c "from src.core.constants import KNOWN_MISSIONS; print(list(KNOWN_MISSIONS.keys()))"` to list known missions
3. Add the mission to `KNOWN_MISSIONS` if it's new

### Empty `data_files` for some samples

Some ISA-Tab assay files have empty "Raw Data File" or "Derived Data File" columns. This is an upstream data completeness issue in the ISA-Tab archive, not a bug. The engine reports only what exists in the source data.

### "No OSD IDs found in column 'OSD_study'"

Your input CSV uses a different column name. Pass the correct name:

```bash
python -m cli.expand_samples input.csv --osd-column osd_id
```

### API timeouts or connection errors

- Retry — transient network issues are common
- The engine caches all successful fetches, so a second run will be faster
- Use `--no-cache` to force a fresh fetch if you suspect stale data

### Forcing fresh data

```bash
# Bypass cache for this run
python -m cli.retrieve_data OSD-242 --no-cache

# Clear all cached data and start fresh
python -m cli.retrieve_data OSD-242 --clear-cache
```

---

## 10. Programmatic Usage

### Using DataRetriever

```python
from pathlib import Path
from src.core.osdr_client import OSDRClient
from src.core.isa_parser import ISAParser
from src.core.mission_resolver import MissionResolver
from src.core.data_retriever import DataRetriever

client = OSDRClient(
    cache_dir=Path("resources/osdr_api/raw"),
    isa_tab_dir=Path("resources/isa_tab"),
)
parser = ISAParser(isa_tab_dir=Path("resources/isa_tab"))
resolver = MissionResolver(client=client, cache_dir=Path("resources/osdr_api/raw"))
retriever = DataRetriever(client=client, parser=parser, resolver=resolver)

# Single OSD
records = retriever.retrieve_osd("OSD-242")

# Entire mission
records = retriever.retrieve_mission("RR-3")

# All known studies
records = retriever.retrieve_all()
```

### Using MissionResolver

```python
# Resolve mission to OSDs
osds = resolver.resolve_mission("RR-3")
# ['OSD-137', 'OSD-162', 'OSD-194']

# Reverse lookup
mission = resolver.get_mission_for_osd("OSD-242")
# 'RR-9'

# List all known missions
missions = resolver.list_known_missions()
# ['BION-M1', 'MHU-2', 'RR-1', 'RR-10', ...]
```

### Using the Scorers

```python
from src.core.informativeness_scorer import SampleInformativenessScorer, MouseInformativenessScorer

# Table 1
sample_scorer = SampleInformativenessScorer()
df = sample_scorer.score(records)  # returns pandas DataFrame

# Table 2
mouse_scorer = MouseInformativenessScorer()
df = mouse_scorer.score(records, "RR-3")  # returns pandas DataFrame
```

### Using the Enrichment Pipeline

```python
from pathlib import Path
from src.core.pipeline import Pipeline, PipelineConfig

config = PipelineConfig(
    input_csv_path=Path("input.csv"),
    output_csv_path=Path("output.csv"),
    provenance_log_path=Path("provenance.json"),
    validation_report_path=Path("validation.txt"),
    use_cache=True,
)
pipeline = Pipeline(config)
result = pipeline.run()

print(f"Enriched {result.enriched_rows}/{result.total_rows} rows")
print(f"Duration: {result.duration_seconds:.1f}s")
```

---

## 11. Environment Variables

| Variable | Description | Default |
|---|---|---|
| `OSDR_PROJECT_ROOT` | Override project root directory | Auto-detected |
| `OSDR_INPUT_CSV` | Default input CSV path | `resources/study_overview_examples/Yeshasvi_2_enriched.csv` |
| `OSDR_OUTPUT_DIR` | Output directory | `outputs/` |
| `OSDR_CACHE_DIR` | API cache directory | `resources/osdr_api/raw/` |
| `OSDR_USE_CACHE` | Enable/disable caching (`true`/`false`) | `true` |

---

## 12. Caching and Performance

### Cache locations

| Directory | Contents | Cleared by |
|---|---|---|
| `resources/osdr_api/raw/` | API JSON responses (`OSD-*.json`) + `mission_registry.json` | `--clear-cache` |
| `resources/isa_tab/` | Extracted ISA-Tab archives (`OSD-*/i_*.txt`, `s_*.txt`, `a_*.txt`) | Manual deletion |

### First run vs subsequent runs

- **First run** for a study: 1-3 seconds (API fetch + ISA-Tab download)
- **Subsequent runs**: < 100ms (reads from local cache)
- **RR-3 (3 OSDs, 53 samples)**: ~2 seconds first run, ~0.7 seconds cached
- **RR-1 (10 OSDs, 302 samples)**: ~15 seconds first run, ~2 seconds cached

### Rate limiting

The NASA APIs have no documented rate limits, but the engine makes at most 4 requests per OSD study (dataset, samples, assays, files list). All responses are cached after the first successful fetch.
