from dagster import job, repository, with_resources

from dagster_meltano import load_assets_from_meltano_project, meltano_install_op, meltano_resource


@job
def meltano_install_job():
    meltano_install_op()()


@repository
def repo():
    return [
        meltano_install_job,
        with_resources(
            load_assets_from_meltano_project("/workspace/meltano"),
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
        ),
    ]
