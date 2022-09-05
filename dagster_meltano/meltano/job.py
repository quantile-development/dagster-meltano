from __future__ import annotations

import os
from typing import TYPE_CHECKING

from dagster import JobDefinition, OpDefinition, job, op
from meltano.core.task_sets import TaskSets

from dagster_meltano.utils import generate_dagster_name

if TYPE_CHECKING:
    from .resource import MeltanoResource


class Job:
    def __init__(self, meltano_task_set: TaskSets, meltano: MeltanoResource):
        self.job = meltano_task_set
        self.meltano = meltano

    @property
    def name(self) -> str:
        return self.job.name

    @property
    def dagster_name(self) -> str:
        """
        Generate a dagster safe name (^[A-Za-z0-9_]+$.)
        """
        return generate_dagster_name(self.name)

    @property
    def create_dagster_op(self) -> OpDefinition:
        """
        Generates a dagster op for each task in the meltano job.
        """

        @op(name=f"run_{self.dagster_name}")
        def dagster_op():
            os.system(f'meltano run {self.name}')

        return dagster_op

    @property
    def create_dagster_job(self) -> JobDefinition:
        @job(name=self.dagster_name)
        def dagster_job():
            self.create_dagster_op()

        return dagster_job
