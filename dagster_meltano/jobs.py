from functools import cache
from typing import List, Optional

from dagster import JobDefinition, ScheduleDefinition

from .meltano.resource import MeltanoResource
from .meltano.schedule import DagsterSchedule


@cache
def load_jobs_from_meltano_project(
    project_dir: Optional[str],
) -> List[JobDefinition]:
    """This function generates dagster jobs for all jobs defined in the Meltano project.

    Args:
        project_dir (Optional[str], optional): The location of the Meltano project. Defaults to os.getenv("MELTANO_PROJECT_ROOT").

    Returns:
        List[JobDefinition]: Returns a list of Dagster JobDefinitions
    """

    meltano_resource = MeltanoResource(project_dir)
    return [job.dagster_job for job in meltano_resource.jobs]


def load_job_schedules_from_meltano_project(
    project_dir: Optional[str],
    meltano_jobs: List[JobDefinition],
) -> List[ScheduleDefinition]:
    """This function generates the schedules (if existing) for all jobs provided Dagster jobs.

    Args:
        project_dir (Optional[str], optional): The location of the Meltano project. Defaults to os.getenv("MELTANO_PROJECT_ROOT").

    Returns:
        List[ScheduleDefinition]: Returns a list of Dagster ScheduleDefinitions
    """

    meltano_resource = MeltanoResource(project_dir)

    meltano_job_schedules = [
        DagsterSchedule(job, meltano_resource).schedule for job in meltano_jobs
    ]

    return [schedule for schedule in meltano_job_schedules if schedule]
