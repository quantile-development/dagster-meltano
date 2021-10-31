"""Smoke test for testing Meltano commands in dagster pipelines using the constructor method."""
import json

from dagster import pipeline, solid

from dagster_meltano.dagster_types import MeltanoEltArgsType, MeltanoEnvVarsType
from dagster_meltano.solids import MeltanoEltSolid


@solid
def elt_args() -> MeltanoEltArgsType:
    return {
        "tap": "tap-csv",
        "target": "target-jsonl",
        "job_id": "csv-to-jsonl",
    }


@solid
def env_vars() -> MeltanoEnvVarsType:
    return {"TAP_CSV__SELECT": json.dumps(["sample.id"])}


@pipeline
def meltano_elt_pipeline():
    MeltanoEltSolid("csv_to_jsonl").solid(elt_args=elt_args(), env_vars=env_vars())
