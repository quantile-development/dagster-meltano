from functools import cache
from typing import List, Optional

from dagster import JobDefinition

from .meltano.resource import MeltanoResource


@cache
def load_jobs_from_meltano_project(
    project_dir: Optional[str],
) -> List[JobDefinition]:
    """This function generates all Jobs it can find in the supplied Meltano project.

    Args:
        project_dir (Optional[str], optional): The location of the Meltano project. Defaults to os.getenv("MELTANO_PROJECT_ROOT").

    Returns:
        List[AssetsDefinition]: Returns a list of all Meltano assets
    """

    meltano_resource = MeltanoResource(project_dir)
    return [job.create_dagster_job for job in meltano_resource.jobs]

    # tap_csv_to_target_postgres_schedule = ScheduleDefinition(
    #     job=tap_csv_to_target_postgres,
    #     cron_schedule="@hourly",
    #     default_status=DefaultScheduleStatus.RUNNING,
    # )

    # return [tap_csv_to_target_postgres, tap_csv_to_target_postgres_schedule]
