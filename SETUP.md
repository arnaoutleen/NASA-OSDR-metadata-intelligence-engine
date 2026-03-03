# SETUP — Installation and Setup Guide

NASA OSDR Metadata Intelligence Engine v2.0.0

---

## 1. Prerequisites

- **Python:** 3.11 or higher (3.12 recommended)
- **OS:** macOS, Linux, or Windows (WSL recommended on Windows)
- **Network:** Internet access to NASA OSDR APIs for the first run. Subsequent runs use locally cached data.
- **Disk space:** ~50 MB for the codebase, plus ~5-50 MB per cached OSD study (ISA-Tab archives)

Check your Python version:

```bash
python3 --version
# Python 3.11.x or higher
```

---

## 2. Installation

### Standard Installation

```bash
git clone https://github.com/yeshasvikamma/NASA-OSDR-metadata-intelligence-engine.git
cd NASA-OSDR-metadata-intelligence-engine
pip install -r requirements.txt
```

### Development Installation (with virtual environment)

```bash
git clone https://github.com/yeshasvikamma/NASA-OSDR-metadata-intelligence-engine.git
cd NASA-OSDR-metadata-intelligence-engine
python3 -m venv venv
source venv/bin/activate    # macOS/Linux
# venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### Dependencies

The project requires only two packages (defined in `requirements.txt`):

| Package | Minimum version | Purpose |
|---|---|---|
| `requests` | >= 2.28.0 | HTTP requests to NASA OSDR APIs |
| `pandas` | >= 1.5.0 | DataFrame operations for ranking tables |

---

## 3. Verifying Installation

Run these checks to confirm everything works:

```bash
# Check imports
python -c "from src.core.data_retriever import DataRetriever; print('Core modules: OK')"
python -c "from src.core.informativeness_scorer import SampleInformativenessScorer; print('Scorer: OK')"
python -c "from src.core.mission_resolver import MissionResolver; print('Resolver: OK')"

# Quick end-to-end test (fetches one study from NASA, takes 2-5 seconds)
python -m cli.retrieve_data OSD-242 -o /tmp/osdr_test.csv
```

If the last command produces output like `Total samples retrieved: 23`, the installation is working.

---

## 4. Directory Structure After Install

After cloning, the project structure looks like this:

```
NASA_discovery/
├── cli/                          # CLI commands (ready to use)
├── src/                          # Source code
│   ├── core/                     #   Core modules
│   ├── intelligence/             #   Pattern recognition
│   ├── validation/               #   Data validation
│   └── utils/                    #   Utilities
├── resources/                    # Data directories
│   ├── test_inputs/demo/         #   Example input CSVs (included)
│   └── schema/                   #   Column schemas (included)
├── outputs/                      # Output directory (created on first run)
├── requirements.txt
├── README.md
├── USAGE.md
└── SETUP.md
```

**On first run**, these directories are auto-created:

| Directory | Created when | Contents |
|---|---|---|
| `resources/osdr_api/raw/` | First API fetch | Cached API JSON responses |
| `resources/isa_tab/OSD-*/` | First ISA-Tab download | Extracted ISA-Tab text files |
| `outputs/retrieval/` | First `retrieve_data` run | Retrieved data CSVs/JSONs |
| `outputs/rankings/` | First `rank_*` run | Ranking table CSVs/JSONs |
| `outputs/enriched_csv/` | First `process_csv` run | Enriched CSVs |
| `outputs/provenance_logs/` | First `process_csv` run | Provenance JSON logs |
| `outputs/validation_reports/` | First `process_csv --validate` run | Validation reports |

---

## 5. Configuration

### Default paths

All paths are configured in `src/utils/config.py` and relative to the project root. No configuration file is needed — defaults work out of the box.

| Setting | Default | Override |
|---|---|---|
| API cache | `resources/osdr_api/raw/` | `OSDR_CACHE_DIR` env var |
| ISA-Tab storage | `resources/isa_tab/` | — |
| Output directory | `outputs/` | `OSDR_OUTPUT_DIR` env var or `-o` flag |
| Request timeout | 60 seconds | — |

### Environment variables

Set these to override defaults:

```bash
export OSDR_CACHE_DIR=/path/to/cache
export OSDR_OUTPUT_DIR=/path/to/outputs
export OSDR_USE_CACHE=true
```

---

## 6. First Run Walkthrough

This walkthrough takes about 30 seconds and confirms the full pipeline works.

### Step 1: Retrieve data for one study

```bash
python -m cli.retrieve_data OSD-242 -o outputs/retrieval/osd242_data.csv
```

Expected output:

```
  [1/1] OSD-242... (23 samples)
Output written to: outputs/retrieval/osd242_data.csv

Total samples retrieved: 23
```

### Step 2: Examine the retrieved data

Open `outputs/retrieval/osd242_data.csv`. You should see 23 rows with columns: `osd`, `source_name`, `sample_name`, `organism`, `material_type`, `measurement_types`, `technology_types`, `device_platforms`, `data_files`.

### Step 3: Rank the samples

```bash
python -m cli.rank_samples OSD-242 -o outputs/rankings/osd242_samples.csv
```

Expected output:

```
  [1/1] OSD-242... (23 samples)

Sample ranking table: 23 rows
Output written to: outputs/rankings/osd242_samples.csv
```

### Step 4: Examine the ranking

Open `outputs/rankings/osd242_samples.csv`. The `num_assays` column shows how many assay types each sample has. The `informativeness_rank` column shows the rank within the study (1 = most data-rich).

### Step 5: Try a mission-level ranking

```bash
python -m cli.rank_mice --mission RR-3 -o outputs/rankings/rr3_mice.csv
```

This fetches 3 OSDs and produces a mouse ranking table. Mice appearing across multiple OSDs will have higher `informativeness_score` values.

---

## 7. Pre-caching (Optional)

To download ISA-Tab archives for common demo studies upfront:

```bash
python -m cli.init_cache
```

This caches 8 demo studies (OSD-102, OSD-202, OSD-242, OSD-479, OSD-477, OSD-546, OSD-661, OSD-207) so subsequent commands run instantly for these studies.

To cache specific studies:

```bash
python -m cli.init_cache --studies OSD-137 OSD-162 OSD-194
```

---

## 8. Updating

### Pulling new code

```bash
git pull
pip install -r requirements.txt
```

### When to clear cache

Clear the cache if:
- NASA has updated the metadata for studies you've previously fetched
- You see stale or incorrect data
- You want to verify reproducibility against fresh API data

```bash
# Clear API cache (forces re-fetch from NASA)
python -m cli.retrieve_data OSD-242 --clear-cache

# Or manually delete cached files
rm -rf resources/osdr_api/raw/OSD-242.json
rm -rf resources/isa_tab/OSD-242/
```

The `mission_registry.json` file in the cache directory stores mission-to-OSD mappings. Delete it to rebuild from the seed registry:

```bash
rm resources/osdr_api/raw/mission_registry.json
```
