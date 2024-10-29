import asyncio
import json
import logging
import os
from functools import cached_property, lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Union

from dagster import DagsterLogManager, resource, Field
from dagster_meltano.exceptions import MeltanoCommandError

from dagster_meltano.job import Job
from dagster_meltano.schedule import Schedule
from dagster_meltano.utils import Singleton
from dagster_shell import execute_shell_command

STDOUT = 1


class MeltanoResource(metaclass=Singleton):
    def __init__(
        self,
        project_dir: str = None,
        meltano_bin: Optional[str] = "meltano",
        retries: int = 0,
    ):
        self.project_dir = str(project_dir)
        self.meltano_bin = meltano_bin
        self.retries = retries

    @property
    def default_env(self) -> Dict[str, str]:
        """The default environment to use when running Meltano commands.

        Returns:
            Dict[str, str]: The environment variables.
        """
        return {
            "MELTANO_CLI_LOG_CONFIG": str(Path(__file__).parent / "logging.yaml"),
            "DBT_USE_COLORS": "false",
            "NO_COLOR": "1",
            **os.environ.copy(),
        }

    def execute_command(
        self,
        command: str,
        env: Dict[str, str],
        logger: Union[logging.Logger, DagsterLogManager] = logging.Logger,
    ) -> str:
        """Execute a Meltano command.

        Args:
            context (OpExecutionContext): The Dagster execution context.
            command (str): The Meltano command to execute.
            env (Dict[str, str]): The environment variables to inject when executing the command.

        Returns:
            str: The output of the command.
        """
        output, exit_code = execute_shell_command(
            f"{self.meltano_bin} {command}",
            env={**self.default_env, **env},
            output_logging="STREAM",
            log=logger,
            cwd=self.project_dir,
        )

        if exit_code != 0:
            raise MeltanoCommandError(
                f"Command '{command}' failed with exit code {exit_code}"
            )

        return output

    async def load_json_from_cli(self, command: List[str]) -> dict:
        """Use the Meltano CLI to load JSON data.
        Use asyncio to run multiple commands concurrently.

        Args:
            command (List[str]): The Meltano command to execute.

        Returns:
            dict: The processed JSON data.
        """
        # Create the subprocess, redirect the standard output into a pipe
        proc = await asyncio.create_subprocess_exec(
            self.meltano_bin,
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.project_dir,
        )

        # Wait for the subprocess to finish
        stdout, stderr = await proc.communicate()

        # Try to load the output as JSON
        try:
            return json.loads(stdout)
        except json.decoder.JSONDecodeError:
            raise ValueError(f"Could not process json: {stdout} {stderr}")

    async def gather_meltano_yaml_information(self):
        jobs, schedules = await asyncio.gather(
            self.load_json_from_cli(["job", "list", "--format=json"]),
            self.load_json_from_cli(["schedule", "list", "--format=json"]),
        )

        return jobs, schedules

    @cached_property
    def meltano_yaml(self) -> dict:
        """Asynchronously load the Meltano jobs and schedules.

        Returns:
            dict: The Meltano jobs and schedules.
        """
        jobs, schedules = asyncio.run(self.gather_meltano_yaml_information())
        return {"jobs": jobs["jobs"], "schedules": schedules["schedules"]}

    @cached_property
    def meltano_jobs(self) -> List[Job]:
        meltano_job_list = self.meltano_yaml["jobs"]
        return [
            Job(
                meltano_job=meltano_job,
                retries=self.retries,
            )
            for meltano_job in meltano_job_list
        ]

    @cached_property
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
            description="The path to the Meltano project.",
            default_value=os.getenv("MELTANO_PROJECT_ROOT", os.getcwd()),
            is_required=False,
        ),
        "retries": Field(
            int,
            description="The number of times to retry a failed job.",
            default_value=0,
            is_required=False,
        ),
    },
)
def meltano_resource(init_context):
    project_dir = init_context.resource_config["project_dir"]
    retries = init_context.resource_config["retries"]

    return MeltanoResource(
        project_dir=project_dir,
        retries=retries,
    )


if __name__ == "__main__":
    meltano_resource = MeltanoResource("/workspace/meltano")
    print(list(meltano_resource.jobs))
    print(meltano_resource.jobs)
