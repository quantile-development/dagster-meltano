from dagster import (
    pipeline,
    InputDefinition, 
    OutputDefinition, 
    Nothing
)
from dagster_meltano.solids import meltano_elt_solid

@pipeline
def meltano_pipeline():
    meltano_elt_solid(
        output_defs=[OutputDefinition(dagster_type=Nothing)],
        tap='tap-csv',
        target='target-jsonl',
        job_id='csv-to-jsonl'
    )