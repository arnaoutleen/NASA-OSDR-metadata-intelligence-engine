# Demo Test Suite

**Sample test files for the NASA OSDR Metadata Intelligence Engine**

These files demonstrate the engine's capabilities and can be used to verify installation.

---

## Quick Start

```bash
# Run a single demo file
python -m cli.process_csv resources/test_inputs/demo/realworld_rodent_research.csv --validate

# Run all demo files
for f in resources/test_inputs/demo/*.csv; do
    python -m cli.process_csv "$f" --validate
done
```

---

## Test Files

| File | Samples | Purpose |
|------|---------|---------|
| `discovery_single_osd.csv` | 3 | Basic single-study enrichment |
| `fallback_invalid_osd.csv` | 2 | Invalid OSD handling |
| `provenance_check.csv` | 2 | Provenance tracking verification |
| `realworld_rodent_research.csv` | 14 | Full rodent study enrichment |
| `realworld_conflicting_patterns.csv` | 10 | Pre-filled value preservation |
| `realworld_study_summary.csv` | 7 | Study-level placeholder testing |

---

## Expected Results

### `discovery_single_osd.csv`
- **OSD-102**: RR-1 kidney samples
- Expected enrichment: strain (C57BL/6J), sex (Female), age (16 weeks)

### `realworld_rodent_research.csv`
- **OSD-102** + **OSD-479**: Multiple Rodent Research samples
- Full auto-discovery of all mouse metadata

### `realworld_conflicting_patterns.csv`
- Pre-filled values intentionally differ from ISA-Tab
- Verifies user data is preserved (not overwritten)

---

## OSD Studies Used

| OSD | Organism | Tissue | Mission |
|-----|----------|--------|---------|
| OSD-102 | Mus musculus | Kidney | Rodent Research 1 |
| OSD-202 | Mus musculus | Brain | Ground study |
| OSD-242 | Mus musculus | Liver | Spaceflight |
| OSD-479 | Mus musculus | Liver | Rodent Research 9 |
| OSD-477 | Rattus norvegicus | Femur | Rat study |
| OSD-546 | Homo sapiens | Bone marrow | Human cells |
| OSD-661 | Mus musculus | Spine | Hindlimb unloading |

---

## Outputs

After running, check:
- `outputs/enriched_csv/` — Enriched CSV files
- `outputs/provenance_logs/` — JSON provenance logs
- `outputs/validation_reports/` — Validation reports

---

## Adding Custom Tests

1. Create a CSV with `osd_id` and `sample_id` columns
2. Use uppercase OSD format: `OSD-###`
3. Run: `python -m cli.process_csv your_file.csv --validate`

