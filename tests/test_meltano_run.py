from dagster import job

from dagster_meltano import meltano_resource, meltano_run_op

from pathlib import Path

MELTANO_PROJECT_TEST_PATH = Path(__file__).parent / "meltano_test_project"


@job(resource_defs={"meltano": meltano_resource})
def meltano_run_job():
    meltano_run_op("meltano --help")()


def test_meltano_run():
    """
    Check if we can run abitrary `meltano run` commands.
    """
    job_response = meltano_run_job.execute_in_process()

    assert job_response.success
