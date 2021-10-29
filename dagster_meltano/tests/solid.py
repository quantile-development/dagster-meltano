"""Smoke test for testing Meltano commands in dagster pipelines using the solid method"""

import json

from dagster import pipeline, solid

from dagster_meltano.solids import meltano_elt_solid


@solid
def elt_args():
    return {
        "tap": "tap-csv",
        "target": "target-jsonl",
        "job_id": "csv-to-jsonl",
    }


@solid
def env_vars():
    return {"TAP_CSV__SELECT": json.dumps(["sample.id"])}


@pipeline
def meltano_solid():
    meltano_elt_solid(elt_args=elt_args(), env_vars=env_vars())
