from dagster import repository, with_resources
from dagster._utils import file_relative_path

from dagster_dbt import dbt_cli_resource
from dagster_meltano import (
    load_assets_from_meltano_project,
    load_jobs_from_meltano_project,
    meltano_resource,
)

MELTANO_PROJECT_DIR = file_relative_path(
    __file__,
    "../",
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
DBT_TARGET_PATH = "/workspace/meltano/transform/target"


@repository
def dagster_meltano():
    return [
        load_jobs_from_meltano_project("/workspace/meltano"),
        with_resources(
            load_assets_from_meltano_project(
                meltano_project_dir=MELTANO_PROJECT_DIR,
                dbt_project_dir=DBT_PROJECT_DIR,
                dbt_profiles_dir=DBT_PROFILES_DIR,
                # dbt_target_dir=DBT_TARGET_PATH,
                dbt_use_build_command=False,
            ),
            {
                "meltano": meltano_resource,
                "dbt": dbt_cli_resource.configured(
                    {
                        "project_dir": DBT_PROJECT_DIR,
                        "profiles_dir": DBT_PROFILES_DIR,
                        # "target_path": DBT_TARGET_PATH,
                        "dbt_executable": "/dagster-testing/.meltano/transformers/dbt-postgres/venv/bin/dbt",
                    },
                ),
            },
        ),
    ]
