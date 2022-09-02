import logging
from dataclasses import dataclass, field
from json import JSONDecoder

from dagster import LoggerDefinition


def extract_json_objects(text, decoder=JSONDecoder()):
    """Find JSON objects in text, and yield the decoded JSON data

    Does not attempt to look for JSON arrays, text, or other JSON types outside
    of a parent JSON object.

    """
    pos = 0
    while True:
        match = text.find('{', pos)
        if match == -1:
            break
        try:
            result, index = decoder.raw_decode(text[match:])
            yield result
            pos = match + index
        except ValueError:
            pos = match + 1


@dataclass
class Metrics:
    record_counts: dict = field(default_factory=dict)


class RepeatHandler(logging.Handler):
    """
    This logger takes all messages from Meltano and re-logs them
    to the Dagster logger.
    """

    def __init__(self, dagster_logger: LoggerDefinition, metrics: Metrics) -> None:
        self.dagster_logger = dagster_logger
        self.metrics = metrics
        super().__init__()

    def emit(self, record):
        if "event" in record.msg:
            message = record.msg["event"]
            self.dagster_logger.info(message)

            for json in extract_json_objects(message):
                self.dagster_logger.debug(json)

                # TODO: Move to a more well-designed system
                if json.get("metric") == "record_count":
                    stream_name = json.get("tags", {}).get("stream")
                    record_count = json.get("value", 0)
                    if stream_name:
                        self.metrics.record_counts[stream_name] = (
                            self.metrics.record_counts.get(stream_name, 0) + record_count
                        )


def add_repeat_handler(logger, dagster_logger) -> RepeatHandler:
    """
    We connect the dagster logger to an incoming logger. In our case the
    Meltano logger.
    """
    metrics = Metrics()
    repeat_handler = RepeatHandler(dagster_logger, metrics)
    logger.addHandler(repeat_handler)

    return repeat_handler
