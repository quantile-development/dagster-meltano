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
)

from dagster_meltano.meltano_invoker import MeltanoInvoker
from dagster_meltano.ops import meltano_run_op as meltano_run_op_factory
from dagster_meltano.utils import generate_dagster_name


class Job:
    def __init__(
        self,
        meltano_job: dict,
        meltano_invoker: MeltanoInvoker,
    ) -> None:
        self.name = meltano_job["job_name"]
        self.tasks = meltano_job["tasks"]
        self.meltano_invoker = meltano_invoker

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
        )
        def dagster_job():
            op_layers = [[], []]
            previous_task_contains_tap = None
            for task in self.tasks:
                meltano_run_op = meltano_run_op_factory(task)
                current_task_contains_tap = self.task_contains_tap(task)

                # # If the task does not contain a tap, we generate a new layer
                # if not self.task_contains_tap(task):
                #     op_layers.append([])

                if not current_task_contains_tap or not previous_task_contains_tap:
                    op_layers.append([])

                # if not current_task_contains_tap and previous_task_contains_tap:

                # op_layers.append([])

                # # When we are in the first layer
                # if len(op_layers) == 1:
                #     op_layers[-1].append(meltano_run_op())
                #     continue

                op_layers[-1].append(meltano_run_op(op_layers[-2]))
                previous_task_contains_tap = current_task_contains_tap

            # logging.warning(op_layers)

            # for op_layer_index, op_layer in enumerate(op_layers)[1:]:
            #     previous_layer = op_layers[op_layer_index - 1]

            #     for
            #     [meltano_run_op(previous_layer) for meltano_run_op in op_layer]

            # meltano_run_op()

            # meltano_run = meltano_run_op(command=task)

            # if previous_done:
            #     previous_done = meltano_run(previous_done)
            # else:
            #     previous_done = meltano_run()

            # if self.task_contains_tap(task):
            #     previous_done.append(previous_done)
            #     continue

        return dagster_job
