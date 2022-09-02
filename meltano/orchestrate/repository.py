from dagster import repository, with_resources
from dagster._utils import file_relative_path

from dagster_dbt import dbt_cli_resource
from dagster_meltano import load_assets_from_meltano_project, meltano_resource

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
        with_resources(
            load_assets_from_meltano_project("/workspace/meltano"),
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
