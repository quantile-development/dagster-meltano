from dagster import pipeline
from dagster_meltano.solids import meltano_elt_solid

@pipeline
def meltano_pipeline():
    meltano_elt_solid(
        tap='tap-csv',
        target='target-jsonl',
        job_id='csv-to-jsonl'
    )