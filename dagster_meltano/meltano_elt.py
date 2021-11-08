"""Class for Meltano ELT command"""

import os
import re
from subprocess import PIPE, STDOUT, Popen
from typing import Generator, List, Optional

from dagster import AssetMaterialization, Failure, SolidExecutionContext

from dagster_meltano.utils import lower_kebab_to_upper_snake_case


class MeltanoCore:
    """Common functionality necessary for all meltano cli commands."""

    def __init__(
        self,
        tap: str,
        target: str,
        tap_config: Optional[dict] = None,
        target_config: Optional[dict] = None,
        env_vars: Optional[dict] = None,
    ) -> None:
        """Initialize a new Meltano ELT process.

        Args:
            tap (str): The name of the Meltano tap.
            target (str): The name of the Meltano target.
            tap_config (Optional[dict]): Additional environment variables relevant to the tap.
            target_config (Optional[dict]): Additional environment variables relevant to the target.
            env_vars (Optional[dict]): Additional environment variables to pass to the
                command context.
        """
        if env_vars is None:
            env_vars = {}
        if tap_config is None:
            tap_config = {}
        if target_config is None:
            target_config = {}

        self._tap = tap
        self._target = target
        self._env_vars = env_vars
        self._tap_config = tap_config
        self._target_config = target_config
        self._command = None
        self._meltano_process = None

    @property
    def _target_config_env_vars(self):
        return {
            lower_kebab_to_upper_snake_case(f"{self._target}_{config_name}"): value
            for config_name, value in self._target_config.items()
        }

    @property
    def _tap_config_env_vars(self):
        return {
            lower_kebab_to_upper_snake_case(f"{self._tap}_{config_name}"): value
            for config_name, value in self._tap_config.items()
        }

    @property
    def meltano_process(self) -> Popen:
        """Creates a subprocess that runs the Meltano ELT command.
        It is started in the Meltano project root, and inherits environment.
        variables from the Dagster environment.

        It injects tap and target configuration by utilizing environment variables.

        Returns:
            Popen: The ELT process.
        """
        # Create a Meltano ELT process if it does not already exists
        if not self._meltano_process:
            self._meltano_process = Popen(
                self._command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=os.getenv(
                    "MELTANO_PROJECT_ROOT"
                ),  # Start the command in the root of the Meltano project
                env={
                    **os.environ,  # Pass all environment variables from the Dagster environment
                    **self._tap_config_env_vars,
                    **self._target_config_env_vars,
                    **self._env_vars,
                },
                start_new_session=True,
            )

        return self._meltano_process

    @property
    def logs(self) -> Generator[str, None, None]:
        """A generator that loops through the stdout and stderr (both routed to stdout).

        Yields:
            Generator[str, None, None]: The lines the ELT command produces.
        """
        # Loop through the stdout of the ELT process
        for line in iter(self.meltano_process.stdout.readline, b""):
            yield line.decode("utf-8").rstrip()

    def run(
        self,
        log: SolidExecutionContext.log,
    ) -> Generator[AssetMaterialization, None, None]:
        """Run `meltano elt` command yielding asset materialization and producing logs.

        Args:
            log (SolidExecutionContext.log): The solid execution context's logger.
        """
        log.info(f"Running Meltano command: {self._command}")

        # Read the Meltano logs, and log them to the Dagster logger
        # Save tracebacks and exceptions for logging as failures
        error_flag = 0
        traceback_str = ""
        for line in self.logs:
            error_flag = 1 if error_flag or re.search("traceback", line, re.IGNORECASE) else 0
            if error_flag:
                traceback_str += "\n" + line
            else:
                log.info(line)

        # Wait for the process to finish
        self.meltano_process.wait()

        return_code = self.meltano_process.returncode

        # If the elt process failed
        if return_code:
            if traceback_str:
                error = f"""The meltano elt failed with code {return_code}. Meltano Traceback:
                {traceback_str}
                \nDagster Traceback:"""
                raise Failure(description=error)
            else:
                error = f"The meltano elt failed with code {return_code}. See logs for more information."
                raise Failure(description=error)
        else:
            log.info(f"Meltano exited with return code {return_code}")


class MeltanoELT(MeltanoCore):
    """Control `meltano elt` command."""

    def __init__(
        self,
        tap: str,
        target: str,
        job_id: str,
        full_refresh: bool,
        tap_config: Optional[dict] = None,
        target_config: Optional[dict] = None,
        env_vars: Optional[dict] = None,
    ) -> None:
        """Initialize a new Meltano ELT process.

        Args:
            job_id (str): The id of the job.
            full_refresh (bool): Whether to ignore existing state.
        """
        super().__init__(tap, target, tap_config, target_config, env_vars)

        self._job_id = job_id
        self._full_refresh = full_refresh
        self._command = self.elt_command

    @property
    def elt_command(self) -> List[str]:
        """Constructs all the parts of the ELT command.

        Returns:
            List[str]: All parts of the ELT command.
        """
        # All default parts of the command
        elt_command = ["meltano", "elt", self._tap, self._target, "--job_id", self._job_id]

        # If the user specified a full refresh
        if self._full_refresh:
            elt_command += ["--full-refresh"]

        self._command = elt_command
        return elt_command
