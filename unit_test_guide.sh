# This is a test run of the code. We are going to run the pipeline from beginning to end on RR-1. We will:

# STEP 1: Metadata Pull
# Download the metadata for all assays EXCEPT western blot and calcium uptake (they're huge and can cause an OOM error if you're running it on your local machine, as opposed to an HPC).

python -m cli.run_full_export --mission RR-1 -o outputs/test_rr1_new --exclude-assay western-blot calcium-uptake

# STEP 2: Assay Query and Exploration
# What assay types are in this file?
python -m cli.query_assays --file outputs/test_rr1_new/assay_parameters_long.csv --list-types

# What parameters exist for RNA-seq?
python -m cli.query_assays --file outputs/test_rr1_new/assay_parameters_long.csv \
    --assay rna_sequencing --list-params

# Show spike-in mix for all RNA-seq samples
python -m cli.query_assays --file outputs/test_rr1_new/assay_parameters_long.csv \
    --assay rna_sequencing --param "Spike-in Mix Number"

# Same but pivoted wide (one column per parameter, one row per sample)
python -m cli.query_assays --file outputs/test_rr1_new/assay_parameters_long.csv \
    --assay rna_sequencing --wide -o outputs/rr1/rnaseq_wide.csv

# Step-by-step interactive prompts — no flags needed
python -m cli.query_assays --file outputs/test_rr1_new/assay_parameters_long.csv --interactive