import os
from dagster import job

from dagster_meltano import meltano_resource, meltano_run_op

from pathlib import Path

MELTANO_PROJECT_TEST_PATH = str(Path(__file__).parent / "meltano_test_project")


@job(resource_defs={"meltano": meltano_resource})
def meltano_run_job():
    meltano_run_op("tap-smoke-test target-jsonl")()


def test_meltano_run():
    """
    Check if we can run abitrary `meltano run` commands.
    """
    job_response = meltano_run_job.execute_in_process(
        {
            "resources": {
                "meltano": {
                    "config": {
                        "project_dir": MELTANO_PROJECT_TEST_PATH,
                    },
                }
            }
        }
    )

    assert job_response.success


def test_meltano_run_using_env():
    """
    Check if we can run abitrary `meltano run` commands, with the project
    root defined using an env variable.
    """
    os.environ["MELTANO_PROJECT_ROOT"] = MELTANO_PROJECT_TEST_PATH
    job_response = meltano_run_job.execute_in_process()

    assert job_response.success
