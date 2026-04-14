# NASA OSDR Metadata Intelligence Engine

**README and USAGE are currently identical.**

_Last updated: April 14, 2026_

## What this repository does

This repository helps you discover, retrieve, and organize metadata from NASA's Open Science Data Repository (OSDR), especially for rodent spaceflight studies. It supports three complementary ways of interrogating the database:

1. **Mission-forward**: start from a mission or OSD study you already know.
2. **Organ-forward**: start from a tissue or body site of interest and ask which studies contain it.
3. **Assay-forward**: start from a data modality or combination of modalities and ask which studies or organs have it.

Once matching studies are identified, the repository can retrieve the detailed metadata, standardize it into analysis-ready tables, and generate informativeness rankings for mice and samples.

This means the tool can meet users where they are:

- "I know the mission"
- "I know the organ"
- "I know the assay"

while still preserving one consistent downstream metadata schema.

---

## Core concepts

### OSD
An **OSD** is a single dataset accession in OSDR, such as `OSD-48`.

### Mission
A **mission** is a broader project grouping multiple OSD studies, such as `RR-1` or `RR-3`.

### Organ-forward discovery
An **organ-forward** query asks which studies contain a given tissue, such as liver, kidney, retina, or thymus.

### Assay-forward discovery
An **assay-forward** query asks which studies contain a given assay type or combination of assay types, such as RNA-seq, proteomics, or methylation.

### Study index cache
The repository can build a lightweight local JSON cache of study-level metadata so discovery queries do not need to hit the live OSDR endpoints every time.

---

## Main workflows

There are now **three main query modes** plus the original full export workflow.

### 1. Mission-forward workflow
Use this when you already know the mission or the exact OSD accession.

Typical questions:
- "Pull metadata for RR-1"
- "Retrieve OSD-48"
- "Export all studies in RR-3"

Main entry points:
- `python -m cli.run_full_export`
- `python -m cli.retrieve_data`
- `python -m cli.process_osd_study`

### 2. Organ-forward workflow
Use this when you know the tissue or body site you care about but not which studies contain it.

Typical questions:
- "Which studies have liver?"
- "Which RR-1 studies have retina?"
- "Which kidney studies have the richest sample coverage?"

Main entry point:
- `python -m cli.rank_by_organ`

### 3. Assay-forward workflow
Use this when you know the modality or modalities you need.

Typical questions:
- "Which studies have RNA-seq?"
- "Which organs have proteomics?"
- "Which studies have both RNA-seq and proteomics?"

Main entry point:
- `python -m cli.rank_by_assay`

### 4. Full metadata export workflow
Use this when you want the full cleaned metadata tables for downstream analysis.

Typical questions:
- "Give me all analysis-ready metadata for RR-1"
- "Export mouse, sample, and assay tables for OSD-48"

Main entry point:
- `python -m cli.run_full_export`

---

## Installation

### Requirements

- Python 3.11 or higher
- Internet connection for live OSDR retrieval and optional cache refresh

### Install dependencies

From the repository root:

```bash
pip install -r requirements.txt
```

### Verify the installation

```bash
python -m cli.run_full_export --help
python -m cli.rank_by_organ --help
python -m cli.rank_by_assay --help
```

If you get a `ModuleNotFoundError`, make sure you are running commands from the repository root.

---

## Recommended mental model

The repository now has two layers:

### 1. Discovery layer
This layer answers fast, high-level questions like:
- what studies exist for liver?
- what studies contain proteomics?
- what organs appear in RR-1?

This layer uses the local **study index cache** when available.

### 2. Retrieval and export layer
This layer downloads or parses the detailed metadata needed to generate:
- mouse metadata
- sample metadata
- assay parameter tables
- informativeness rankings

This layer remains the source of truth for downstream scientific use.

In other words:
- the **cache** is for discovery
- the **full retrieval** is for final outputs

---

## The study index cache

The study index is a lightweight JSON file storing study-level information for fast discovery.

Default location:

```text
resources/osdr_api/study_index.json
```

Typical cached fields include:
- `osd_id`
- `mission`
- `title`
- `description`
- `organs`
- `assays`
- `sample_count`
- `mouse_count`
- `retrievable`
- `last_seen`

### Build or refresh the study index

```bash
python -m cli.build_study_index
python -m cli.build_study_index --force
```

This command is useful before organ-forward or assay-forward querying.

---

## Mission-forward usage

### Full export for a mission

```bash
python -m cli.run_full_export --mission RR-1 -o outputs/rr1
```

### Full export for one or more OSD studies

```bash
python -m cli.run_full_export --osd OSD-48 -o outputs/osd48
python -m cli.run_full_export --osd OSD-48 OSD-87 OSD-168 -o outputs/osd_subset
```

### Full export from a CSV of studies

```bash
python -m cli.run_full_export \
  --input resources/test_inputs/demo/realworld_rodent_research.csv \
  -o outputs/from_csv
```

### Lower-level mission or study retrieval

```bash
python -m cli.retrieve_data --mission RR-1
python -m cli.retrieve_data OSD-48
python -m cli.process_osd_study OSD-48
```

### Restricting assays in a full export

To exclude selected assay types:

```bash
python -m cli.run_full_export --mission RR-1 -o outputs/rr1 \
  --exclude-assay western-blot calcium-uptake
```

To include only selected assay types:

```bash
python -m cli.run_full_export --mission RR-1 -o outputs/rr1 \
  --include-assay rna-seq
```

`--include-assay` and `--exclude-assay` are mutually exclusive.

---

## Organ-forward usage

The organ-forward CLI discovers studies with a given organ, optionally constrained to a mission, then can produce ranking-style outputs on the matched studies.

### List discoverable organs

```bash
python -m cli.rank_by_organ --list-organs
```

### Rank studies and outputs for one organ

```bash
python -m cli.rank_by_organ --organ liver
```

### Restrict an organ query to one mission

```bash
python -m cli.rank_by_organ --organ retina --mission RR-1
```

### Control output paths

```bash
python -m cli.rank_by_organ \
  --organ kidney \
  --mouse-output outputs/rankings/kidney_mice.csv \
  --sample-output outputs/rankings/kidney_samples.csv \
  --assay-output outputs/rankings/kidney_assays.csv \
  --studies-output outputs/rankings/kidney_studies.csv
```

### Refresh the study index before querying

```bash
python -m cli.rank_by_organ --organ liver --refresh-index
```

### What organ-forward outputs can include

Depending on flags and implementation in your local branch, organ-forward queries can generate:
- matched studies summary
- mouse ranking
- sample ranking
- assay summary or assay ranking-style table

---

## Assay-forward usage

The assay-forward CLI discovers studies by assay type or combinations of assay types, optionally constrained by mission or organ.

### List discoverable assays

```bash
python -m cli.rank_by_assay --list-assays
```

### Query one assay

```bash
python -m cli.rank_by_assay --assay RNA-Seq
```

### Query a combination of assays

Require all listed assays:

```bash
python -m cli.rank_by_assay --assay RNA-Seq --assay Proteomics --match all
```

Allow any listed assay:

```bash
python -m cli.rank_by_assay --assay RNA-Seq --assay Proteomics --match any
```

### Constrain assay queries by organ or mission

```bash
python -m cli.rank_by_assay --assay RNA-Seq --organ liver --mission RR-1
```

### Retrieve downstream rankings for assay-matched studies

```bash
python -m cli.rank_by_assay --assay RNA-Seq --retrieve
```

### What assay-forward outputs can include

Depending on flags and implementation in your local branch, assay-forward queries can generate:
- matched studies summary
- organ summary for matched studies
- mouse ranking for retrieved matched studies
- sample ranking for retrieved matched studies

---

## Assay parameter exploration

After a full export, use `query_assays.py` to interrogate the detailed assay parameter table.

### List assay categories in a long-format table

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --list-types
```

### List all parameters for one assay category

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --assay rna_sequencing \
  --list-params
```

### Query one specific parameter

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --assay rna_sequencing \
  --param "Spike-in Mix Number"
```

### Restrict the query to one study

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --assay protein_mass_spec \
  --osd OSD-48
```

### Export wide-format results

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --assay rna_sequencing \
  --wide \
  -o outputs/rr1/rnaseq_params_wide.csv
```

### Interactive mode

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --interactive
```

---

## Main outputs from the full export workflow

The full export workflow typically produces:

- `mouse_metadata.csv`
- `sample_metadata.csv`
- `assay_parameters_long.csv`
- `assay_parameters_wide.csv`
- `mouse_ranking.csv`
- `data_manifest.json`

### `mouse_metadata.csv`
One row per animal.

### `sample_metadata.csv`
One row per sample × assay combination or equivalent unified metadata row, depending on the export schema in your branch.

### `assay_parameters_long.csv`
Long-format assay technical metadata.

### `assay_parameters_wide.csv`
Wide-format assay technical metadata.

### `mouse_ranking.csv`
Mouse informativeness ranking.

### `data_manifest.json`
Tracks pull timestamps, study hashes, and freshness metadata for re-runs.

---

## Re-running and freshness checking

If you rerun the same export into the same output directory, the repository can compare current study content against cached or previously pulled state.

Example:

```bash
python -m cli.run_full_export --mission RR-1 -o outputs/rr1
python -m cli.run_full_export --mission RR-1 -o outputs/rr1
```

This can help distinguish:
- unchanged studies
- newly updated studies
- newly discovered studies

The exact freshness mechanism depends on the workflow:
- full export workflows may use manifest and ISA-Tab hashing
- study-index workflows may use cache age and optional live refresh

---

## Directory structure

A typical repository layout looks like this:

```text
NASA-OSDR-metadata-intelligence-engine/
├── cli/
│   ├── run_full_export.py
│   ├── retrieve_data.py
│   ├── process_osd_study.py
│   ├── query_assays.py
│   ├── build_study_index.py
│   ├── rank_by_organ.py
│   ├── rank_by_assay.py
│   └── ...
├── src/
│   ├── core/
│   │   ├── osdr_client.py
│   │   ├── mission_resolver.py
│   │   ├── study_index.py
│   │   ├── export_tables.py
│   │   ├── informativeness_scorer.py
│   │   └── ...
│   └── utils/
├── resources/
│   ├── isa_tab/
│   └── osdr_api/
│       ├── raw/
│       └── study_index.json
└── outputs/
```

---

## Suggested user paths

### Path A: I know the mission

```bash
python -m cli.run_full_export --mission RR-1 -o outputs/rr1
```

### Path B: I know the organ but not the study

```bash
python -m cli.build_study_index
python -m cli.rank_by_organ --organ liver
```

### Path C: I know the assay but not the study

```bash
python -m cli.build_study_index
python -m cli.rank_by_assay --assay RNA-Seq
```

### Path D: I want to inspect assay technical confounds

```bash
python -m cli.query_assays \
  --file outputs/rr1/assay_parameters_long.csv \
  --assay rna_sequencing \
  --param "Library Layout"
```

---

## Troubleshooting

### The command cannot find a module
Make sure you are running commands from the repository root.

### A study or mission is not found
Check that the OSD or mission identifier is valid and publicly available.

### Organ-forward or assay-forward queries feel stale
Rebuild or refresh the study index:

```bash
python -m cli.build_study_index --force
```

or:

```bash
python -m cli.rank_by_organ --organ liver --refresh-index
python -m cli.rank_by_assay --assay RNA-Seq --refresh-index
```

### My discovery query finds studies but rankings are empty
That usually means the study-level cache identified plausible matches, but the detailed retrieval layer did not find corresponding sample-level records under the requested filters. Treat the cache as a discovery aid, not the final truth layer.

### I only want one part of the workflow
That is fine. The repository is modular:
- use mission-forward for direct retrieval
- use organ-forward for tissue-driven discovery
- use assay-forward for modality-driven discovery
- use full export for analysis-ready tables

---

## In one sentence

This repository is now a **metadata discovery and export engine** for NASA OSDR studies that supports mission-first, organ-first, and assay-first querying while preserving a unified downstream export workflow.
