from dagster import asset, repository, with_resources
from dagster._utils import file_relative_path

from dagster_dbt import dbt_cli_resource
from dagster_meltano import (
    load_assets_from_meltano_project,
    load_jobs_from_meltano_project,
    meltano_resource,
)

DBT_PROJECT_DIR = file_relative_path(
    __file__,
    "../transform",
)
DBT_PROFILES_DIR = file_relative_path(
    __file__,
    "../transform/profiles",
)
DBT_EXECUTABLE = file_relative_path(
    __file__,
    "../.meltano/transformers/dbt-postgres/venv/bin/dbt",
)


@repository
def repo():
    return [
        # tap_csv_to_target_postgres_schedule,
        # tap_csv_to_target_postgres,
        load_jobs_from_meltano_project("/workspace/meltano"),
        with_resources(
            load_assets_from_meltano_project("/workspace/meltano"),
            # meltano_assets("tap-github"),
            # + meltano_assets("tap-lightspeed")
            # + [machine_learning_model],
            {
                "dbt": dbt_cli_resource.configured(
                    {
                        "project_dir": DBT_PROJECT_DIR,
                        "profiles_dir": DBT_PROFILES_DIR,
                        "dbt_executable": DBT_EXECUTABLE,
                    },
                ),
                "meltano": meltano_resource,
            },
        ),
    ]
