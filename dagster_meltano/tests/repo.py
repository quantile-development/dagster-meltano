from dagster import repository

from dagster_meltano.tests.meltano_elt_pipeline import meltano_elt_pipeline


@repository
def repository_example():
    return {"pipelines": {"meltano_elt_pipeline": lambda: meltano_elt_pipeline}}
