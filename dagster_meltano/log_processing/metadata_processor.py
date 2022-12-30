import asyncio
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from json import JSONDecoder
from typing import Any, Iterator, List, Optional

from dagster_meltano.log_processing import LogProcessor


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


class Metric:
    def __init__(self, body: dict) -> None:
        self.body = body

    @property
    def is_record_count(self) -> bool:
        return self.body.get("metric") == "record_count"

    @property
    def value(self) -> int:
        return self.body.get("value")

    @property
    def tags(self) -> dict:
        return self.body.get("tags")

    @property
    def stream_name(self) -> str:
        return self.tags.get("stream") or self.tags.get("endpoint")


class MetadataLogProcessor(LogProcessor):
    metrics: List[Metric] = []

    def find_metrics(self, log_line: str) -> Iterator[Metric]:
        for json_object in extract_json_objects(log_line):
            yield Metric(json_object)

    def process_line(self, log_line: str):
        if not log_line:
            return

        log_function = self.dagster_logger.info

        try:
            log_line = json.loads(log_line)
        except json.decoder.JSONDecodeError:
            log_function(log_line)
            return

        event = log_line.get("event", log_line)

        if log_line.get("level") == "debug":
            log_function = self.dagster_logger.debug

        log_function(event)

        self.metrics.extend(self.find_metrics(event))

    @property
    def results(self) -> Optional[Any]:
        return self.metrics
