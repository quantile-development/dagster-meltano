from dagster_meltano.log_processing import LogProcessor


class PassthroughLogProcessor(LogProcessor):
    """
    A log processor that passes all log lines to the Dagster logger.
    """

    def process_line(self, log_line: str):
        """Pass the log line to Dagster.

        Args:
            log_line (str): The log line to pass.
        """
        self.dagster_logger.info(log_line)
