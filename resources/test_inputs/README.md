# Test Inputs

Test files for the NASA OSDR Metadata Intelligence Engine.

## Directory Structure

```
test_inputs/
└── demo/                    # Demo test suite
    ├── README.md            # Test file documentation
    ├── discovery_single_osd.csv
    ├── fallback_invalid_osd.csv
    ├── provenance_check.csv
    ├── realworld_rodent_research.csv
    ├── realworld_conflicting_patterns.csv
    └── realworld_study_summary.csv
```

## Quick Start

```bash
# Run a demo file
python -m cli.process_csv resources/test_inputs/demo/realworld_rodent_research.csv --validate

# Run all demo files
for f in resources/test_inputs/demo/*.csv; do
    python -m cli.process_csv "$f" --validate
done
```

## Adding Custom Test Files

1. Create a CSV with at minimum `osd_id` and `sample_id` columns
2. Use uppercase OSD format: `OSD-###`
3. Place in `demo/` or create a new subdirectory
4. Run with the CLI: `python -m cli.process_csv your_file.csv --validate`

See `demo/README.md` for detailed test case documentation.
