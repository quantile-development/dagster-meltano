from dagster import repository

from dagster_meltano.tests.constructor import meltano_constructor
from dagster_meltano.tests.solid import meltano_solid


@repository
def repository_example():
    return {
        "pipelines": {
            "meltano_constructor": lambda: meltano_constructor,
            # "meltano_solid": lambda: meltano_solid,
        }
    }
