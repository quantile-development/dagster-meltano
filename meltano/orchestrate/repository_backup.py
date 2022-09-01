import csv
import os

import requests
from dagster import (
    Out,
    Output,
    asset,
    define_asset_job,
    job,
    multi_asset,
    op,
    repository,
    with_resources,
)
from dagster._utils import file_relative_path
from meltano.core.db import project_engine
from meltano.core.project import Project
from meltano.core.select_service import SelectService
from orchestrate.dagster_meltano import (  # meltano_assets,
    load_assets_from_meltano_project,
    meltano_resource,
)

from dagster_dbt import (
    dbt_cli_resource,
    dbt_docs_generate_op,
    dbt_ls_op,
    load_assets_from_dbt_project,
)

# @op
# def print_env():
#     # Print environment variables
#     for key, value in os.environ.items():
#         print(f"{key}={value}")

# DBT_PROJECT_DIR = file_relative_path(__file__, "../transform")
# DBT_PROFILES_DIR = file_relative_path(__file__, "../transform/profiles")
# DBT_TARGET_PATH = file_relative_path(__file__, "../.meltano/transformers/dbt/target")


# @asset(compute_kind="python", group_name="forecasting")
# def machine_learning_model(comments_per_day):
#     return None


# def node_group_name(node_info):
#     if len(node_info.get("fqn", [])) >= 3:
#         return "_".join(node_info["fqn"][1:-1])

#     return "dbt"


# dbt_assets = load_assets_from_dbt_project(
#     DBT_PROJECT_DIR,
#     DBT_PROFILES_DIR,
#     # target_dir=DBT_TARGET_PATH,
#     node_info_to_group_fn=node_group_name,
# )


load_assets_from_meltano_project()


@repository
def dagster_testing():
    return [
        with_resources(
            load_assets_from_meltano_project(),
            # meltano_assets("tap-github"),
            # + meltano_assets("tap-lightspeed")
            # + [machine_learning_model],
            {
                # "dbt": dbt_cli_resource.configured(
                #     {
                #         "project_dir": DBT_PROJECT_DIR,
                #         "profiles_dir": DBT_PROFILES_DIR,
                #         "dbt_executable": "/dagster-testing/.meltano/transformers/dbt-postgres/venv/bin/dbt",
                #     },
                # ),
                "meltano": meltano_resource,
            },
        )
    ]


# from dagster._utils import file_relative_path
# from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

# DBT_PROJECT_DIR = file_relative_path(__file__, "../transform")
# DBT_PROFILES_DIR = file_relative_path(__file__, "../transform/profiles")


# @repository
# def assets_dbt_python():
#     return with_resources(
#         meltano_assets,
#         resource_defs={
#             # # this io_manager allows us to load dbt models as pandas dataframes
#             # "io_manager": duckdb_io_manager.configured(
#             #     {"duckdb_path": os.path.join(DBT_PROJECT_DIR, "example.duckdb")}
#             # ),
#             # # this io_manager is responsible for storing/loading our pickled machine learning model
#             # "model_io_manager": fs_io_manager,
#             # # this resource is used to execute dbt cli commands
#             "dbt": dbt_cli_resource.configured(
#                 {
#                     "project_dir": DBT_PROJECT_DIR,
#                     "profiles_dir": DBT_PROFILES_DIR,
#                     "dbt_executable": "/dagster-testing/.meltano/transformers/dbt-postgres/venv/bin/dbt",
#                 }
#             ),
#         },
#     )
#     # + [
#     #     # run everything once a week, but update the forecast model daily
#     #     ScheduleDefinition(job=everything_job, cron_schedule="@weekly"),
#     #     ScheduleDefinition(job=forecast_job, cron_schedule="@daily"),
#     # ]
