import os
from subprocess import PIPE, Popen, STDOUT
from typing import Generator, List, Optional
import signal

def pre_exec():
    # Restore default signal disposition and invoke setsid
    for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
        if hasattr(signal, sig):
            signal.signal(getattr(signal, sig), signal.SIG_DFL)

    os.setsid()

class MeltanoELT:
    def __init__(
        self,
        tap: str,
        target: str,
        job_id: str,
        full_refresh: bool,
        env_vars: Optional[dict],
    ) -> None:
        """Initialize a new Meltano ELT process.

        Args:
            tap (str): The name of the Meltano tap.
            target (str): The name of the Meltano target.
            job_id (str): The id of the job.
            full_refresh (bool): Whether to ignore existing state.
            env_vars (Optional[dict]): Additional environment variables to pass to the
                command context.
        """
        self.tap = tap
        self.target = target
        self.job_id = job_id
        self.full_refresh = full_refresh
        self._elt_process = None
        self.env_vars = env_vars

    @property
    def elt_command(self) -> List[str]:
        """Constructs all the parts of the ELT command.

        Returns:
            List[str]: All parts of the ELT command.
        """
        # All default parts of the command
        elt_command = [
            'meltano', 
            'elt', 
            self.tap,
            self.target,
            '--job_id',
            self.job_id
        ]

        # If the user specified a full refresh
        if self.full_refresh:
            elt_command += ['--full-refresh']

        return elt_command

    @property
    def elt_process(self) -> Popen:
        """Creates a subprocess that runs the Meltano ELT command.
        It is started in the Meltano project root, and inherits environment.
        variables from the Dagster environment. 

        It injects tap and target configuration by utilizing environment variables.

        Returns:
            Popen: The ELT process.
        """

        # Create a Meltano ELT process if it does not already exists
        if not self._elt_process:
            self._elt_process = Popen(
                self.elt_command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=os.getenv('MELTANO_PROJECT_ROOT'),  # Start the command in the root of the Meltano project
                env={
                    **os.environ,  # Pass all environment variables from the Dagster environment
                    **self.env_vars
                },
                preexec_fn=pre_exec,
            )

        return self._elt_process

    @property
    def logs(self) -> Generator[str, None, None]:
        """A generator that loops through the stdout and stderr (both routed to stdout).

        Yields:
            Generator[str, None, None]: The lines the ELT command produces.
        """
        # Loop through the stdout of the ELT process
        for line in iter(self.elt_process.stdout.readline, b""):
            yield line.decode("utf-8").rstrip()