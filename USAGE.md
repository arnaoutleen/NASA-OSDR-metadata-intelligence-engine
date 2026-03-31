# NASA OSDR Metadata Intelligence Engine — User Guide

NOTE: Readme and the User Guide are Identical for now 
last edit by leen 3/31/2026 10:35am

## Table of Contents

1. [What This Tool Does](#1-what-this-tool-does)
2. [Setup](#2-setup)
3. [Understanding the Data Sources](#3-understanding-the-data-sources)
4. [Core Workflow: Pulling and Exporting Data](#4-core-workflow-pulling-and-exporting-data)
5. [Output Files — What You Get](#5-output-files--what-you-get)
6. [Re-running and Freshness Checking](#6-re-running-and-freshness-checking)
7. [Exploring Assay Parameters](#7-exploring-assay-parameters)
8. [Mouse Informativeness Ranking](#8-mouse-informativeness-ranking)
9. [Directory Structure Reference](#9-directory-structure-reference)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. What This Tool Does

This engine pulls metadata from NASA's Open Science Data Repository (OSDR) for spaceflight mouse studies and organises it into clean, analysis-ready tables. For each study you request it:

- Downloads the ISA-Tab archive (the authoritative metadata format used by OSDR)
- Parses mouse-level biology: strain, sex, age, spaceflight status, habitat, diet, light cycle, euthanasia method, duration in space
- Parses sample-level data: which organ, which assay type, assay name (RNA-seq, mass spec, DNA methylation, etc.)
- Parses assay-specific technical parameters per assay type
- Ranks mice by how informationally rich they are (number of organs × assay types)
- Tracks when data was pulled and detects if OSDR has updated a study since your last run

The output is five CSV/JSON files per run that you can open directly in Excel or load into Python/R.

---

## 2. Setup

### Requirements

- Python **3.11 or higher** (3.12 recommended)
- An internet connection to reach the NASA OSDR API

### Install dependencies

From the root of the repository:

```bash
pip install -r requirements.txt
```

`requirements.txt` contains:

```
requests>=2.28.0
pandas>=1.5.0
```

### Verify the installation

```bash
python -m cli.run_full_export --help
```

You should see the full usage text. If you get a `ModuleNotFoundError`, make sure you are running from the repository root (the folder that contains `src/` and `cli/`).

---

## 3. Understanding the Data Sources

Before running anything it helps to know what the tool fetches and where it stores things.

**ISA-Tab archives** are the ground truth. Each OSD study on OSDR ships with a zip archive containing:

| File | Contains |
|------|----------|
| `i_Investigation.txt` | Study-level metadata: payload (SpaceX-4), mission (RR-1), PI, dates |
| `s_OSD-NNN.txt` | Sample sheet: one row per animal × organ. Contains strain, sex, age, habitat, diet, light cycle, euthanasia method, duration |
| `a_OSD-NNN_<assay-type>_*.txt` | Assay file: one row per sample for that assay. Contains library layout, sequencing instrument, spike-in mix, MS analyzer, etc. |

**Where files are cached locally:**

```
resources/
  isa_tab/          ← downloaded ISA-Tab archives, one folder per OSD study
    OSD-48/
      i_Investigation.txt
      s_OSD-48.txt
      a_OSD-48_transcription-profiling_rna-sequencing_Illumina.txt
      ...
  osdr_api/
    raw/            ← cached API JSON responses, one file per OSD study
      OSD-48.json
      OSD-102.json
      ...
```

The tool downloads ISA-Tab automatically on first run. Subsequent runs reuse the local cache unless you force a refresh.

**Key concepts:**

- **Payload** — the rocket/vehicle that carried the mice (e.g. `SpaceX-4`, `SpaceX-8`). Comes from `Comment[Mission Name]` in the investigation file.
- **Mission** — the research project (e.g. `RR-1`, `RR-3`). Comes from `Comment[Project Identifier]`.
- **OSD** — a single dataset within a mission. One mission typically has multiple OSDs (different organs or PIs).

Mouse and sample IDs are prefixed with the payload name (`SpaceX-4_RR1_FLT_M23`) so IDs stay unique when you combine data across studies.

---

## 4. Core Workflow: Pulling and Exporting Data

Everything runs through one command: `python -m cli.run_full_export`

You must supply exactly one of `--osd`, `--mission`, or `--input`.

### Option A — One or more specific studies

```bash
python -m cli.run_full_export --osd OSD-48 -o outputs/osd48
```

```bash
python -m cli.run_full_export --osd OSD-48 OSD-87 OSD-168 -o outputs/rr1_subset
```

Use this when you know which OSD numbers you want. The tool verifies each ID against the live API before expanding.

### Option B — An entire mission

```bash
python -m cli.run_full_export --mission RR-1 -o outputs/rr1
```

```bash
python -m cli.run_full_export --mission RR-3 -o outputs/rr3
```

The tool resolves the mission name to its constituent OSD IDs, verifies each one is reachable, then processes all of them. Known missions are listed in `src/core/constants.py` (`KNOWN_MISSIONS`). If a mission isn't there yet the tool falls back to scanning the full OSDR study list via the API, which is slower.

Common mission names:

| Mission | Studies |
|---------|---------|
| `RR-1`  | OSD-48, OSD-100–105, OSD-168, OSD-488, OSD-489 |
| `RR-3`  | OSD-137, OSD-162, OSD-194 |
| `RR-10` | OSD-462, OSD-466, OSD-563, OSD-564 |

### Option C — From a CSV file

If you have a spreadsheet listing OSD IDs (the format used in the original metadata engine workflow):

```bash
python -m cli.run_full_export \
  --input resources/test_inputs/demo/realworld_rodent_research.csv \
  -o outputs/from_csv
```

The CSV must have a column named `OSD_study`. A column named `RR_mission` is optional but used as the mission label if present. You can override the column names with `--osd-col` and `--mission-col`.

### EXTRA: excluding certain assays

Sometimes, running metadata pulls for huge assays like Calcium Uptake and Western Blot can overload the script and cause an OOM error. Therefore, you can exclude them using `--exclude-assay`. Simply do this:

```bash
python -m cli.run_full_export --mission RR-1 -o outputs/rr1 \
--exclude-assay western-blot calcium-uptake
```

And if you only want to include a very specific assay type, like RNA-seq only, you can specify it using `--include-assay`. Like this:

```bash
python -m cli.run_full_export --mission RR-1 -o outputs/rr1 \
--include-assay rna-seq
```

#### IMPORTANT NOTE:  `--include-assay` and `--exclude-assay` are mutually exclusive. Do NOT run them together. Run only one at a time.


### What happens when you run it

```
Verifying OSD IDs via live API …
  Verifying OSD-48 ... OK
  Verifying OSD-102 ... OK

  Expanding OSD-48 ... 32 samples  [NEW/UPDATED — 2025-06-01T14:22:00Z]
  Expanding OSD-102 ... 16 samples  [unchanged — keeping 2025-01-15T10:30:00Z]

Building export tables from 48 records …

============================================================
EXPORT COMPLETE
============================================================
  Mice:            47 rows  →  outputs/rr1/mouse_metadata.csv
  Samples:         48 rows  →  outputs/rr1/sample_metadata.csv
  Assay params:   216 rows  →  outputs/rr1/assay_parameters_long.csv
  Assay wide:          →  outputs/rr1/assay_parameters_wide.csv
  Mouse ranking:   30 mice  →  outputs/rr1/mouse_ranking.csv
  Manifest:            →  outputs/rr1/data_manifest.json

Data freshness:
  OSD-48       UPDATED     pulled_at=2025-06-01T14:22:00Z
  OSD-102      unchanged   pulled_at=2025-01-15T10:30:00Z

Fill-rate (mouse_metadata):
  strain                   : 47/47
  sex                      : 47/47
  age                      : 41/47
  spaceflight_status       : 47/47
  duration                 : 14/47
  habitat                  : 47/47
  animal_source            : 47/47
  genotype                 : 0/47
```

`duration` is intentionally blank for ground/basal control animals — those rows have `spaceflight_status = ground` and a placeholder `0 day` is suppressed.

---

## 5. Output Files — What You Get

All output files share the first four columns:

| Column | Example | Meaning |
|--------|---------|---------|
| `osd_id` | `OSD-48` | OSDR accession number |
| `pulled_at` | `2025-01-15T10:30:00Z` | When this study's data was fetched (UTC) |
| `payload` | `SpaceX-4` | Rocket/vehicle that carried the mice |
| `mission` | `RR-1` | Research project identifier |

### `mouse_metadata.csv`

One row per unique animal. This is your animal-level table.

| Column | Example | Notes |
|--------|---------|-------|
| `mouse_id` | `SpaceX-4_RR1_FLT_M23` | Payload-prefixed, globally unique |
| `source_name` | `RR1_FLT_M23` | Original ID from ISA-Tab |
| `strain` | `C57BL/6J` | |
| `animal_source` | `Jackson Laboratory` | Vendor |
| `genotype` | `Wild` | Blank if wild-type was not stated |
| `sex` | `Female` | |
| `age` | `16 week` | Unit always appended |
| `spaceflight_status` | `spaceflight` or `ground` | |
| `duration` | `37 day` | Blank for ground controls |
| `habitat` | `Rodent Habitat` or `Vivarium Cage` | |
| `light_cycle` | `12 h light/dark cycle` | |
| `diet` | `Nutrient Upgraded Rodent Food Bar (NuRFB)` | |
| `feeding_schedule` | `ad libitum` | |
| `euthanasia_method` | `Euthasol` | |
| `n_samples_linked` | `3` | How many rows in sample_metadata link to this mouse |

### `sample_metadata.csv`

One row per sample × assay type combination. A single mouse with liver (mass spec + RNA-seq) produces two rows.

Key columns beyond the shared four:

| Column | Example | Notes |
|--------|---------|-------|
| `sample_id` | `SpaceX-4_Mmus_C57-6J_LVR_FLT_Rep1_M23` | Globally unique |
| `mouse_id` | `SpaceX-4_RR1_FLT_M23` | Links to mouse_metadata |
| `mouse_strain` | `C57BL/6J` | |
| `mouse_sex` | `Female` | |
| `age` | `16 week` | |
| `space_or_ground` | `spaceflight` | |
| `material_type` | `Left kidney` | Organ |
| `assay_category` | `rna_sequencing` | Normalised category |
| `assay_name` | `rna-seq` or `TMT 11-plex` | Exact name from ISA-Tab |
| `is_rnaseq` | `True` / `False` | Boolean flag for each assay type |
| `is_mass_spec` | `True` / `False` | |
| `is_dna_methylation` | `True` / `False` | |
| *(and 9 more `is_*` flags)* | | |

**Assay category values** you will see:

| `assay_category` | What it means |
|-----------------|---------------|
| `rna_sequencing` | RNA-seq / targeted transcriptome sequencing |
| `dna_methylation` | WGBS, RRBS |
| `protein_mass_spec` | Orbitrap, TMT, phosphoproteomics |
| `rna_methylation` | m6A, whole-transcriptome bisulfite |
| `metabolite_profiling` | GC-MS, LC-MS/MS |
| `chromatin_accessibility` | ATAC-seq |
| `behavior` | Ethovision |
| `echocardiogram` | Ultrasound cardiac imaging |
| `molecular_cellular_imaging` | Microscopy, histology |
| `protein_quantification` | Western blot |
| `atpase_activity` | Spectrophotometry |
| `calcium_uptake` | Fluorescence plate reader |

### `assay_parameters_long.csv`

One row per sample × parameter. This is the detailed technical metadata table — see [Section 7](#7-exploring-assay-parameters) for how to navigate it.

Key columns:

| Column | Example |
|--------|---------|
| `assay_category` | `rna_sequencing` |
| `assay_name` | `rna-seq` |
| `parameter_name` | `Parameter Value[Spike-in Mix Number]` |
| `parameter_value` | `ERCC Mix 1` |

### `assay_parameters_wide.csv`

The same data pivoted so each parameter becomes its own column and each row is one sample × assay category. Easier for Excel filtering; `assay_parameters_long.csv` is better for programmatic use.

### `mouse_ranking.csv`

One row per non-pooled mouse, ranked by informativeness within each study. See [Section 8](#8-mouse-informativeness-ranking).

### `data_manifest.json`

Records the ISA-Tab file hash and pull timestamp for each study. Used automatically on the next run to detect whether data has changed. You don't need to edit this file.

```json
{
  "OSD-48": {
    "pulled_at": "2025-01-15T10:30:00Z",
    "isa_hash": "f3266c9d481a...",
    "n_samples": 32,
    "changed": true
  }
}
```

---

## 6. Re-running and Freshness Checking

When you run the tool a second time pointing at the same output directory, it automatically checks whether the OSDR data has changed since your last pull.

```bash
# First run
python -m cli.run_full_export --mission RR-1 -o outputs/rr1

# Second run, weeks later
python -m cli.run_full_export --mission RR-1 -o outputs/rr1
```

On the second run you will see one of two messages per study:

```
  Expanding OSD-48 ... 32 samples  [unchanged — keeping 2025-01-15T10:30:00Z]
  Expanding OSD-102 ... 16 samples  [NEW/UPDATED — 2026-03-01T09:15:00Z]
```

**How it works:** the tool computes a SHA-256 hash of all ISA-Tab files for each study and compares it against `data_manifest.json`. If the hash matches, the original `pulled_at` timestamp is preserved in all output tables. If anything changed, the study is re-expanded and the timestamp is updated.

This means your `pulled_at` column tells you the true date of the data you analysed, not just when you last ran the script.

---

## 7. Exploring Assay Parameters

The `assay_parameters_long.csv` file contains detailed technical metadata for every assay. Use `query_assays.py` to navigate it without opening Excel.

### Step 1 — See what assay types are in your file

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --list-types
```

Output:

```
ASSAY CATEGORY                       PARAMS   SAMPLES  EXAMPLE PARAMETERS
----------------------------------------------------------------------------------------------------
  protein_mass_spec                        4       24  MS Assay Name | Parameter Value[Analyzer] | ...
  rna_methylation                          8       12  Assay Name | Comment[Extraction Method] | ...
  rna_sequencing                           3       12  Parameter Value[Library Layout] | ...
```

### Step 2 — See all parameters for one assay type

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --assay rna_sequencing \
  --list-params
```

Output:

```
Parameters for assay category: 'rna_sequencing'
PARAMETER NAME                                              VALUES   SAMPLES  EXAMPLE VALUES
-----------------------------------------------------------------------------------------------------------------
  Parameter Value[Library Layout]                               12        12  PAIRED
  Parameter Value[Library Selection]                            12        12  polyA enrichment | ribo-depletion
  Parameter Value[Spike-in Mix Number]                          12        12  ERCC Mix 1 | ERCC Mix 2
```

### Step 3 — Query a specific parameter

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --assay rna_sequencing \
  --param "Spike-in Mix Number"
```

This is especially useful for detecting confounds — if `ERCC Mix 1` perfectly correlates with spaceflight and `ERCC Mix 2` with ground controls, that's a technical confound that will invalidate differential expression results.

### Step 4 — Filter to one study

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --assay protein_mass_spec \
  --osd OSD-48
```

### Step 5 — Get a wide table (one column per parameter)

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --assay rna_sequencing \
  --wide \
  -o outputs/rr1/rnaseq_params_wide.csv
```

Output columns look like:

```
osd_id | pulled_at | payload | mission | sample_id | mouse_id | assay_category | assay_name
| Parameter Value[Library Layout] | Parameter Value[Spike-in Mix Number] | ...
```

### Step 6 — Interactive mode (no flags needed)

If you are not sure what to filter on, use interactive mode:

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --interactive
```

It will prompt you step by step: choose an assay category, then a parameter, then optionally a study, then choose long or wide output.

### Combining filters

All filters can be combined freely. Partial string matching is used for `--assay` and `--param`:

```bash
# "rna" matches both rna_sequencing and rna_methylation
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --assay rna \
  --param "Library" \
  --osd OSD-102 \
  -o outputs/rr1/rna_library_osd102.csv
```

---

## 8. Mouse Informativeness Ranking

`mouse_ranking.csv` is produced automatically every time you run `run_full_export`. It tells you which mice have the richest data coverage.

### How the score is calculated

For each non-pooled mouse:

```
score = (number of distinct organs) × (number of distinct assay types)
        + log₂(1 + number of sample rows)
```

A mouse with liver + kidney measured by RNA-seq + mass spec scores much higher than a mouse with only liver measured by mass spec alone. The log term gives a small bonus for additional replicates without letting outliers dominate.

### Reading the ranking file

| Column | Example | Meaning |
|--------|---------|---------|
| `mouse_id` | `SpaceX-4_RR1_FLT_M25` | |
| `osd_id` | `OSD-48` | |
| `mission` | `RR-1` | |
| `n_organs` | `2` | Distinct tissues sampled |
| `organs` | `Left kidney \| Liver` | |
| `n_assay_types` | `2` | Distinct assay categories |
| `assay_types` | `rna_methylation \| rna_sequencing` | |
| `assay_names` | `RNA-Bisulfite-Seq \| rna-seq` | |
| `n_sample_rows` | `2` | Rows in sample_metadata for this mouse |
| `informativeness_score` | `5.585` | Higher = more informative |
| `informativeness_rank` | `1` | 1 = most informative within (payload, mission, osd_id) |

Ranks are computed **within each study group** (`payload + mission + osd_id`), so mice from different missions are not compared against each other directly.

Pooled samples (e.g. `Pool of all FLT samples`) are excluded from ranking automatically.

### Practical use

The ranking is most useful when selecting mice for downstream multi-omics analysis. Mice ranked 1 have data across the most organs and assay types, making them the best candidates for integration analyses (e.g. MOFA+, multi-omics correlation).

---

## 9. Directory Structure Reference

```
NASA-OSDR-metadata-intelligence-engine/
│
├── cli/
│   ├── run_full_export.py     ← main entry point (use this)
│   ├── query_assays.py        ← assay parameter browser
│   ├── init_cache.py          ← pre-download ISA-Tab for demo studies
│   └── ...                    ← other utilities (see below)
│
├── src/
│   ├── core/
│   │   ├── sample_expander.py       ← ISA-Tab → SampleRow objects
│   │   ├── isa_parser.py            ← parses i_*.txt, s_*.txt, a_*.txt
│   │   ├── export_tables.py         ← builds the three output DataFrames
│   │   ├── informativeness_scorer.py← mouse ranking logic
│   │   ├── mission_resolver.py      ← mission name → OSD IDs
│   │   └── osdr_client.py           ← API calls + caching
│   └── utils/
│       └── export_schema.py         ← column name definitions
│
├── resources/
│   ├── isa_tab/               ← downloaded ISA-Tab archives (auto-created)
│   └── osdr_api/raw/          ← cached API JSON (auto-created)
│
└── outputs/                   ← your output goes here (auto-created)
    └── rr1/
        ├── mouse_metadata.csv
        ├── sample_metadata.csv
        ├── assay_parameters_long.csv
        ├── assay_parameters_wide.csv
        ├── mouse_ranking.csv
        └── data_manifest.json
```

### Other CLI tools

These are available but not part of the main workflow:

| Script | Purpose |
|--------|---------|
| `python -m cli.init_cache` | Pre-download ISA-Tab for a set of demo studies so the first export run is fast |
| `python -m cli.process_osd_study OSD-48` | Inspect a single study's metadata in the terminal |
| `python -m cli.retrieve_data OSD-48` | Lower-level data retrieval (produces raw CSV, not the cleaned export tables) |

---

## 10. Troubleshooting

### "NOT FOUND — OSD-NNN does not exist or is not reachable"

The tool verified the OSD ID against the live OSDR API and got no result. Check:
- The OSD number is correct (visit `https://osdr.nasa.gov` and search)
- Your internet connection can reach `osdr.nasa.gov` and `visualization.osdr.nasa.gov`
- The study is publicly released (some studies are embargoed)

### "Mission 'RR-X' not in local registry — scanning live API"

The mission name isn't in the hardcoded `KNOWN_MISSIONS` list in `src/core/constants.py`. The tool will scan all OSDR studies to find matching ones, which can take a few minutes. If you run this mission frequently, add it to `KNOWN_MISSIONS` to make subsequent runs instant.

### `pulled_at` is not updating even though data changed

The freshness check compares a hash of the **locally cached ISA-Tab files**, not the live API. If you want to force a fresh download of ISA-Tab for a study, delete its folder from `resources/isa_tab/OSD-NNN/` and re-run.

### Many fields are blank in `mouse_metadata.csv`

The fill rate depends entirely on what the study deposited in its ISA-Tab. Fields like `genotype` are blank in most RR studies because all animals are wild-type and it wasn't stated explicitly. `duration` is intentionally blank for ground/basal controls.

### `assay_parameters_long.csv` is empty for a study

Some assay types have no technical parameters recorded in the ISA-Tab assay file. This is common for histology and some behavioural assays. The tool only captures what OSDR actually deposited.

### "cannot insert pulled_at, already exists" error

Update `cli/run_full_export.py` to the latest version — this was a bug fixed in a prior session.

### Memory or performance issues with large missions

For missions with 10+ studies (e.g. RR-10), the full export can take 5–10 minutes on first run while ISA-Tab archives are downloaded. Subsequent runs reuse the local cache and are much faster. If you only need a subset of studies, use `--osd` instead of `--mission`.
