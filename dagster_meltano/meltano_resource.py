import asyncio
import os
from functools import lru_cache
from typing import Dict, List, Optional

from dagster import resource, Field

from dagster_meltano.job import Job
from dagster_meltano.log_processing.json_processor import JsonLogProcessor
from dagster_meltano.meltano_invoker import MeltanoInvoker
from dagster_meltano.schedule import Schedule
from dagster_meltano.utils import Singleton

STDOUT = 1


class MeltanoResource(metaclass=Singleton):
    def __init__(
        self,
        project_dir: str = None,
        meltano_bin: Optional[str] = "meltano",
    ):
        self.project_dir = project_dir
        self.meltano_bin = meltano_bin
        self.meltano_invoker = MeltanoInvoker(
            bin=meltano_bin,
            cwd=project_dir,
            log_level="info",  # TODO: Get this from the resource config
        )

    async def load_json_from_cli(self, command: List[str]) -> dict:
        _, log_results = await self.meltano_invoker.exec(
            None,
            JsonLogProcessor,
            command,
        )
        return log_results[STDOUT]

    async def gather_meltano_yaml_information(self):
        jobs, schedules = await asyncio.gather(
            self.load_json_from_cli(["job", "list", "--format=json"]),
            self.load_json_from_cli(["schedule", "list", "--format=json"]),
        )

        return jobs, schedules

    @property
    @lru_cache
    def meltano_yaml(self):
        jobs, schedules = asyncio.run(self.gather_meltano_yaml_information())
        return {"jobs": jobs["jobs"], "schedules": schedules["schedules"]}

    @property
    @lru_cache
    def meltano_jobs(self) -> List[Job]:
        meltano_job_list = self.meltano_yaml["jobs"]
        return [
            Job(
                meltano_job,
                self.meltano_invoker,
            )
            for meltano_job in meltano_job_list
        ]

    @property
    @lru_cache
    def meltano_schedules(self) -> List[Schedule]:
        meltano_schedule_list = self.meltano_yaml["schedules"]["job"]
        schedule_list = [
            Schedule(meltano_schedule) for meltano_schedule in meltano_schedule_list
        ]
        return schedule_list

    @property
    def meltano_job_schedules(self) -> Dict[str, Schedule]:
        return {schedule.job_name: schedule for schedule in self.meltano_schedules}

    @property
    def jobs(self) -> List[dict]:
        for meltano_job in self.meltano_jobs:
            yield meltano_job.dagster_job

        for meltano_schedule in self.meltano_schedules:
            yield meltano_schedule.dagster_schedule


@resource(
    description="A resource that corresponds to a Meltano project.",
    config_schema={
        "project_dir": Field(
            str,
            default_value=os.getenv("MELTANO_PROJECT_ROOT", os.getcwd()),
            is_required=False,
        )
    },
)
def meltano_resource(init_context):
    project_dir = init_context.resource_config["project_dir"]
    return MeltanoResource(project_dir)


if __name__ == "__main__":
    meltano_resource = MeltanoResource("/workspace/meltano")
    print(list(meltano_resource.jobs))
    print(meltano_resource.jobs)
