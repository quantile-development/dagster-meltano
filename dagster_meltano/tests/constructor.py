"""Smoke test for testing Meltano commands in dagster pipelines using the constructor method."""

from dagster import pipeline, solid

from dagster_meltano.dagster_types import MeltanoEltArgsType
from dagster_meltano.solids_new import MeltanoEltSolid


@solid
def elt_args() -> MeltanoEltArgsType:
    return {
        # "tap": "tap-csv",
        "target": "target-jsonl",
        "job_id": "csv-to-jsonl",
    }


@pipeline
def meltano_constructor():
    MeltanoEltSolid("csv_to_jsonl", tap="tap-co2").solid(elt_args=elt_args())
