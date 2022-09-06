from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional

from dagster import DefaultScheduleStatus, JobDefinition, OpDefinition, ScheduleDefinition, job, op
from meltano.core.schedule import Schedule
from meltano.core.task_sets import TaskSets

from dagster_meltano.utils import generate_dagster_name

if TYPE_CHECKING:
    from .resource import MeltanoResource


class Job:
    def __init__(self, meltano_job: TaskSets, meltano: MeltanoResource):
        self.meltano_job = meltano_job
        self.meltano = meltano

    @property
    def meltano_name(self) -> str:
        """
        The job name as specified in the Meltano project.
        """
        return self.meltano_job.name

    @property
    def dagster_name(self) -> str:
        """
        Generate a dagster safe name (^[A-Za-z0-9_]+$.)
        """
        return generate_dagster_name(self.meltano_name)

    @property
    def create_dagster_op(self) -> OpDefinition:
        """
        Generates a dagster op for the meltano job.
        """
        # TODO: Move this to the `dagster_meltano/ops.py` file.
        @op(name=f"run_{self.dagster_name}")
        def dagster_op():
            # TODO: This needs a different runner
            os.system(f'meltano run {self.meltano_name}')

        return dagster_op

    @property
    def dagster_job(self) -> JobDefinition:
        """
        Generates a Dagster job that runs the Meltano job using a Dagster op.
        """

        @job(name=self.dagster_name)
        def dagster_job():
            self.create_dagster_op()

        return dagster_job

    @property
    def meltano_schedule(self) -> Optional[Schedule]:
        """
        Returns a Meltano schedule, if a Meltano schedule was linked to
        this Meltano job.
        """
        return self.meltano.schedules.get(self.meltano_name)

    @property
    def dagster_schedule(self) -> Optional[ScheduleDefinition]:
        """
        Returns a Dagster schedule if a Meltano schedule is defined.
        """
        if self.meltano_schedule:
            return ScheduleDefinition(
                job_name=self.dagster_name,
                cron_schedule=self.meltano_schedule.cron_interval,
                default_status=DefaultScheduleStatus.RUNNING,
            )
