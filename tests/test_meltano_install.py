from dagster import job

from dagster_meltano import meltano_resource, meltano_install_op

from pathlib import Path

MELTANO_PROJECT_TEST_PATH = str(Path(__file__).parent / "meltano_test_project")


@job(resource_defs={"meltano": meltano_resource})
def install_job():
    meltano_install_op()


def test_meltano_install():
    """
    Check if the meltano install command works
    """
    job_response = install_job.execute_in_process(
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
