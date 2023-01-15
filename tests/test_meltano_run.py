import os
import subprocess
import pytest
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


def test_meltano_run_using_env_root():
    """
    Check if we can run abitrary `meltano run` commands, with the project
    root defined using an env variable.
    """
    os.environ["MELTANO_PROJECT_ROOT"] = MELTANO_PROJECT_TEST_PATH
    job_response = meltano_run_job.execute_in_process()

    assert job_response.success


def test_meltano_run_injecting_env():
    """
    Check if we can inject environment variables into the `meltano run` command.
    We test this by injecting a non existing Meltano environment, which should
    cause the command to fail.
    """
    with pytest.raises(subprocess.CalledProcessError):
        meltano_run_job.execute_in_process(
            {
                "resources": {
                    "meltano": {
                        "config": {
                            "project_dir": MELTANO_PROJECT_TEST_PATH,
                        },
                    }
                },
                "ops": {
                    "tap_smoke_test_target_jsonl": {
                        "config": {
                            "env": {
                                "MELTANO_ENVIRONMENT": "non_existing_env",
                            }
                        }
                    }
                },
            }
        )
