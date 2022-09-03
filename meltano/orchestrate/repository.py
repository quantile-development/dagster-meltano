from dagster import repository, with_resources

from dagster_meltano import (
    load_assets_from_meltano_project,
    load_jobs_from_meltano_project,
    meltano_resource,
)


@repository
def repo():
    return [
        load_jobs_from_meltano_project("/workspace/meltano"),
        with_resources(
            load_assets_from_meltano_project("/workspace/meltano"),
            {
                "meltano": meltano_resource,
            },
        ),
    ]
