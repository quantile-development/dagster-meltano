import asyncio
import json
import logging
from functools import lru_cache

from dagster import (
    In,
    JobDefinition,
    Nothing,
    OpDefinition,
    OpExecutionContext,
    get_dagster_logger,
    job,
    op,
    RetryPolicy,
)

from dagster_meltano.ops import meltano_run_op as meltano_run_op_factory
from dagster_meltano.utils import generate_dagster_name


class Job:
    def __init__(self, meltano_job: dict, retries: int = 0) -> None:
        self.name = meltano_job["job_name"]
        self.tasks = meltano_job["tasks"]
        self.retries = retries

    @property
    def dagster_name(self) -> str:
        return generate_dagster_name(self.name)

    def task_contains_tap(self, task: str) -> bool:
        """Check whether the supplied task contains a tap."""
        return "tap-" in task

    @property
    def dagster_job(self) -> JobDefinition:
        # We need to import the `meltano_resource` here to prevent circular imports.
        from dagster_meltano.meltano_resource import meltano_resource

        @job(
            name=self.dagster_name,
            description=f"Runs the `{self.name}` job from Meltano.",
            resource_defs={"meltano": meltano_resource},
            op_retry_policy=RetryPolicy(max_retries=self.retries),
        )
        def dagster_job():
            op_layers = [[], []]
            previous_task_contains_tap = None
            for task in self.tasks:
                meltano_run_op = meltano_run_op_factory(task)
                current_task_contains_tap = self.task_contains_tap(task)

                if not current_task_contains_tap or not previous_task_contains_tap:
                    op_layers.append([])

                op_layers[-1].append(meltano_run_op(after=op_layers[-2]))
                previous_task_contains_tap = current_task_contains_tap

        return dagster_job
