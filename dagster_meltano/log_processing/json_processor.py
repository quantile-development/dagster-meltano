import json
from typing import Any, Optional

from dagster_meltano.log_processing import LogProcessor


class JsonLogProcessor(LogProcessor):
    """
    A log processor that tries to read JSON from the log lines.
    """

    log_lines = ""

    def process_line(self, log_line: str):
        self.log_lines += log_line

    @property
    def results(self) -> Optional[Any]:
        try:
            return json.loads(self.log_lines)
        except json.decoder.JSONDecodeError:
            raise ValueError(f"Could not process json: {self.log_lines}")
