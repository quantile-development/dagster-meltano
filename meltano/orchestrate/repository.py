from dagster import repository, with_resources
from dagster_meltano import (
    load_assets_from_meltano_project,
    load_job_schedules_from_meltano_project,
    load_jobs_from_meltano_project,
    meltano_resource,
)


@repository
def repo():

    meltano_jobs = load_jobs_from_meltano_project("/workspace/meltano")
    meltano_job_schedules = load_job_schedules_from_meltano_project(
        "/workspace/meltano",
        meltano_jobs,
    )

    return [
        meltano_jobs,
        meltano_job_schedules,
        with_resources(
            load_assets_from_meltano_project("/workspace/meltano"),
            {
                "meltano": meltano_resource,
            },
        ),
    ]
