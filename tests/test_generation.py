from dagster import JobDefinition, ScheduleDefinition

from dagster_meltano import load_jobs_from_meltano_project

from pathlib import Path

MELTANO_PROJECT_TEST_PATH = Path(__file__).parent / "meltano_test_project"


def test_job_and_schedule_returned():
    """
    Check if the generator function correctly returns the jobs and schedules.
    """
    (job, schedule) = load_jobs_from_meltano_project(MELTANO_PROJECT_TEST_PATH)

    assert isinstance(job, JobDefinition)
    assert isinstance(schedule, ScheduleDefinition)

    assert job.name == "smoke_job"
    assert schedule.name == "daily_smoke_job"


def test_job():
    """
    Run the job using Dagster and check for failures.
    """
    (job, _schedule) = load_jobs_from_meltano_project(MELTANO_PROJECT_TEST_PATH)

    job_response = job.execute_in_process()

    assert job_response.success
    assert job_response.output_for_node("tap_smoke_test_target_jsonl") == None


def test_schedule():
    """
    Make sure the generated cron schedule has the correct interval
    """
    (_job, schedule) = load_jobs_from_meltano_project(MELTANO_PROJECT_TEST_PATH)

    assert schedule.cron_schedule == "0 0 * * *"
