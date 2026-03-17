from pathlib import Path
from src.core.pipeline import Pipeline, PipelineConfig

config = PipelineConfig(
    input_csv_path=Path("rr1_pipeline_input.csv"),
    output_csv_path=Path("outputs/rr1_enriched.csv"),
    provenance_log_path=Path("outputs/rr1_provenance.json"),
)

pipeline = Pipeline(config)
pipeline.run()
