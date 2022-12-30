import asyncio
import json
import os
import subprocess
from asyncio.subprocess import Process
from pathlib import Path
from typing import IO, Any, Callable, Dict, List, Optional, Tuple, Union

from dagster import get_dagster_logger

from dagster_meltano.log_processing import LogProcessor
from dagster_meltano.log_processing.passthrough_processor import PassthroughLogProcessor

# log = structlog.get_logger()
log = get_dagster_logger()


class MeltanoInvoker:
    """Invoker utility class for invoking subprocesses."""

    def __init__(
        self,
        bin: str = "meltano",
        cwd: str = None,
        log_level: str = "info",
        env: Optional[Dict[str, Any]] = {},
    ) -> None:
        """Minimal invoker for running subprocesses.

        Args:
            bin: The path/name of the binary to run.
            cwd: The working directory to run from.
            env: Env to use when calling Popen, defaults to current os.environ if None.
        """
        self.bin = bin
        self.cwd = cwd
        self.env = {
            **os.environ.copy(),
            "MELTANO_CLI_LOG_CONFIG": Path(__file__).parent / "logging.yaml",
            "MELTANO_CLI_LOG_LEVEL": log_level,
            "DBT_USE_COLORS": "false",
            "NO_COLOR": "1",
            **env,
        }

    def run(
        self,
        *args: Union[str, bytes],
        stdout: Union[None, int, IO] = subprocess.PIPE,
        stderr: Union[None, int, IO] = subprocess.STDOUT,
        text: bool = True,
        **kwargs: Any,
    ) -> subprocess.Popen:
        """Run a subprocess. Simple wrapper around subprocess.run.

        Note that output from stdout and stderr is NOT logged automatically. Especially
        useful when you want to run a command, but don't care about its output and only
        care about its return code.

        stdout and stderr by default are set up to use subprocess.PIPE. If you do not
        want to capture io from the subprocess use subprocess.DEVNULL to discard it.

        The Invoker's at env and cwd are used when calling `subprocess.run`. If you want
        to override these you're likely better served using `subprocess.run` directly.

        Lastly note that this method is blocking AND `subprocess.run` is called with
        `check=True`. This means that if the subprocess fails a `CalledProcessError`
        will be raised.

        Args:
            *args: The arguments to pass to the subprocess.
            stdout: The stdout stream to use.
            stderr: The stderr stream to use.
            text: If true, decode stdin, stdout and stderr using the system default.
            **kwargs: Additional keyword arguments to pass to subprocess.run.

        Returns:
            The completed process.
        """
        return subprocess.Popen(
            [self.bin, *args],
            cwd=self.cwd,
            env=self.env,
            stdout=stdout,
            stderr=stderr,
            text=text,
            **kwargs,
        )

    async def exec(
        self,
        sub_command: Union[str, None] = None,
        log_processor: Optional[LogProcessor] = PassthroughLogProcessor,
        *args: Union[str, bytes],
    ) -> Tuple[asyncio.subprocess.Process, List[any]]:
        popen_args = []
        if sub_command:
            popen_args.append(sub_command)
        if args:
            popen_args.extend(*args)

        process = await asyncio.create_subprocess_exec(
            self.bin,
            *popen_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.cwd,
            env=self.env,
        )

        log_results = await asyncio.gather(
            asyncio.create_task(
                log_processor(process.stderr, log_type="stderr").process_logs()
            ),
            asyncio.create_task(
                log_processor(process.stdout, log_type="stdout").process_logs()
            ),
            return_exceptions=True,
        )

        # Raise first exception if any
        for log_result in log_results:
            if isinstance(log_result, Exception):
                raise log_result

        await process.wait()
        return process, log_results

    def run_and_log(
        self,
        sub_command: Union[str, None] = None,
        log_processor: Optional[LogProcessor] = None,
        *args: Union[str, bytes],
    ) -> Tuple[Any, Any]:
        """Run a subprocess and stream the output to the logger.

        Note that output from stdout and stderr IS logged. Best used when you want
        to run a command and stream the output to a user.

        Args:
            sub_command: The subcommand to run.
            log_processor: Gets called for each log line that is being processed.
            *args: The arguments to pass to the subprocess.

        Raises:
            CalledProcessError: If the subprocess failed.
        """
        process, log_results = asyncio.run(self.exec(sub_command, log_processor, *args))
        if process.returncode:
            raise subprocess.CalledProcessError(
                process.returncode, cmd=self.bin, stderr=None
            )

        return log_results
