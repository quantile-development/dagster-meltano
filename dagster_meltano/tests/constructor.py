"""Smoke test for testing Meltano commands in dagster pipelines using the constructor method."""

import json

from dagster import Nothing, OutputDefinition, pipeline

from dagster_meltano.solids import meltano_elt_constructor


@pipeline
def meltano_constructor():
    meltano_elt_constructor(
        output_defs=[OutputDefinition(dagster_type=Nothing)],
        tap="tap-csv",
        target="target-jsonl",
        job_id="csv-to-jsonl",
        env_vars={"TAP_CSV__SELECT": json.dumps(["sample.id"])},
    )
