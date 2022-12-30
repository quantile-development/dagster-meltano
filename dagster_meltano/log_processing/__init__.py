import asyncio
from abc import ABC, abstractmethod
from typing import Any, Optional

from dagster import get_dagster_logger


class LogProcessor(ABC):
    """
    A generic log processor.
    """

    def __init__(self, reader: asyncio.streams.StreamReader, log_type: str) -> dict:
        self.reader = reader
        self.log_type = log_type
        self.dagster_logger = get_dagster_logger()

    async def process_logs(self):
        """
        A method that starts processing the logs.
        """
        while True:
            # If there are no more logs to process
            if self.reader.at_eof():
                break

            log_line = await self.reader.readline()
            log_line = log_line.decode("utf-8").rstrip()
            self.process_line(log_line)

            await asyncio.sleep(0)

        return self.results

    @abstractmethod
    def process_line(self, log_line: str):
        """Process a single log line.

        Args:
            log_line (str): A single log line.
        """
        ...

    def results(self) -> Optional[Any]:
        """Return optional results from the log processor.

        Returns:
            Optional[Any]: _description_
        """
        return None
