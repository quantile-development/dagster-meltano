from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from dagster import DefaultScheduleStatus, JobDefinition, ScheduleDefinition
from meltano.core.schedule import Schedule

from dagster_meltano.utils import generate_meltano_name

if TYPE_CHECKING:
    from .resource import MeltanoResource


class DagsterSchedule:
    def __init__(self, dagster_job: JobDefinition, meltano: MeltanoResource):
        self.dagster_job = dagster_job
        self.meltano = meltano

    @property
    def meltano_job_name(self):
        return generate_meltano_name(self.dagster_job.name)

    @property
    def meltano_schedule(self) -> Optional[Schedule]:
        """
        The job schedule (if it exists) as defined in the Meltano project.
        """
        try:
            schedule = [
                schedule
                for schedule in self.meltano.schedules
                if schedule.job == self.meltano_job_name
            ][0]
        except IndexError:
            schedule = None
        return schedule

    @property
    def schedule(self) -> ScheduleDefinition:
        if not self.meltano_schedule:
            return None

        dagster_schedule = ScheduleDefinition(
            job=self.dagster_job,
            cron_schedule=self.meltano_schedule.interval,
            default_status=DefaultScheduleStatus.RUNNING,
        )
        return dagster_schedule
