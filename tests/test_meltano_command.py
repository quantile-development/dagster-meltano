from dagster import job

from dagster_meltano import meltano_resource, meltano_command_op

from pathlib import Path

MELTANO_PROJECT_TEST_PATH = str(Path(__file__).parent / "meltano_test_project")


@job(resource_defs={"meltano": meltano_resource})
def meltano_command_job():
    meltano_command_op("install extractor tap-smoke-test", "custom_job_name")()


def test_meltano_command():
    """
    Check if we can run abitrary Meltano commands.
    """
    job_response = meltano_command_job.execute_in_process(
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
