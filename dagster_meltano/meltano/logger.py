import logging


class RepeatHandler(logging.Handler):
    def __init__(self, dagster_logger) -> None:
        self.dagster_logger = dagster_logger
        super().__init__()

    def emit(self, record):
        if "event" in record.msg:
            self.dagster_logger.info(record.msg["event"])


def add_repeat_logger(logger, dagster_logger) -> None:
    """
    We connect the dagster logger to an incoming logger. In our case the
    Meltano logger.
    """
    logger.addHandler(RepeatHandler(dagster_logger))
