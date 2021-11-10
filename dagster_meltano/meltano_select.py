from typing import List, Optional

from dagster import Failure, SolidExecutionContext

from dagster_meltano.meltano_elt import MeltanoCore


class MeltanoSelect(MeltanoCore):
    """Control `meltano select` command."""

    def __init__(
        self,
        tap: str,
        target: Optional[str] = None,
        tap_config: Optional[dict] = None,
        target_config: Optional[dict] = None,
        env_vars: Optional[dict] = None,
        patterns: Optional[List[List[str]]] = None,
    ) -> None:
        """Initialize a new Meltano ELT process.

        Args:
            patterns (Optional[List[List[str]]]): The entity pattern(s) to use in select
                command. Each item in the parent list should contain a list of 2-3 elements.
                If two elements, the first element is the entity pattern and the second is the
                attribute pattern. If three elements the first element should be '--exclude'
                or '--remove'.
        """
        super().__init__(tap, target, tap_config, target_config, env_vars)

        if patterns is None:
            patterns = []

        self._patterns: list = patterns
        self._command = None

    def select_command(self, pattern: List[str]) -> List[str]:
        """Constructs all the parts of the `select` command.
        See https://meltano.com/docs/command-line-interface.html#select.

        Args:
            pattern (List[str]): The list of cli arguments to append to the command.
        Returns:
            List[str]: All parts of the ELT command.
        """
        # All default parts of the command
        select_command = ["meltano", "select", self._tap]

        # If the user specified to list the current selected tap attributes
        select_command += pattern

        self._command = select_command
        return select_command

    def run_select_commands(self, log: SolidExecutionContext.log) -> List[List[str]]:
        """Iterates through all select commands provided."""
        cmds = []
        for pattern in self._patterns:
            if isinstance(pattern, list):
                cmds.append(self.select_command(pattern))
                self._meltano_process = None  # resets the process
                self.run(log=log)
            else:
                raise Failure("'patterns' parameter must be a list of lists")

        return cmds
