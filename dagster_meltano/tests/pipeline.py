"""Smoke test for testing Meltano commands in dagster pipelines"""

import json

from dagster import Nothing, OutputDefinition, pipeline

from dagster_meltano.solids import meltano_elt_solid


@pipeline
def meltano_pipeline():
    meltano_elt_solid(
        output_defs=[OutputDefinition(dagster_type=Nothing)],
        tap="tap-csv",
        target="target-jsonl",
        job_id="csv-to-jsonl",
        env_vars={"TAP_CSV__SELECT": json.dumps(["sample.id"])},
    )
