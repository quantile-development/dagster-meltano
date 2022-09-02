from __future__ import annotations

import asyncio
import logging
from contextlib import closing
from datetime import datetime
from typing import TYPE_CHECKING

from dagster import (
    AssetKey,
    AssetsDefinition,
    Nothing,
    OpExecutionContext,
    Out,
    Output,
    multi_asset,
)
from meltano.cli.elt import _elt_context_builder, _run_extract_load
from meltano.core.job import Job
from meltano.core.logging import JobLoggingService, OutputLogger
from meltano.core.plugin import PluginDefinition
from meltano.core.plugin.singer.catalog import SelectionType
from meltano.core.select_service import SelectService

from ..utils import generate_dagster_name

if TYPE_CHECKING:
    from .resource import MeltanoResource

from .extract_load import extract_load_factory


def run_streams(context: OpExecutionContext):
    log = context.log

    log.info(context.resources.meltano)
    meltano: MeltanoResource = context.resources.meltano
    log.info(context.selected_output_names)

    class RepeatHandler(logging.Handler):
        def emit(self, record):
            if "event" in record.msg:
                log.info(record.msg["event"])

    logging.getLogger("meltano").addHandler(RepeatHandler())

    select_filter = list(context.selected_output_names)
    log.info(f"Selected streams: {select_filter}")

    state_id = None
    extractor = "tap-csv"
    loader = "target-postgres"
    full_refresh = True

    job = Job(
        job_name=state_id
        or f'{datetime.utcnow().strftime("%Y-%m-%dT%H%M%S")}--{extractor}--{loader}'
    )

    with closing(meltano.session()) as session:
        plugins_service = meltano.plugins_service
        context_builder = _elt_context_builder(
            meltano.project,
            job,
            session,
            extractor,
            loader,
            transform="skip",
            full_refresh=full_refresh,
            select_filter=select_filter,
            plugins_service=plugins_service,
        ).context()

        job_logging_service = JobLoggingService(meltano.project)

        log_file = job_logging_service.generate_log_name(job.job_name, job.run_id)
        output_logger = OutputLogger(log_file)

        log.debug(f"Logging to {log_file}")

        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            _run_extract_load(
                log,
                context_builder,
                output_logger,
            )
        )

    for stream_name in context.selected_output_names:
        yield Output(value=None, output_name=stream_name)


class Extractor:
    def __init__(self, extractor: PluginDefinition, meltano: MeltanoResource):
        self.extractor = extractor
        self.meltano = meltano

    @property
    def name(self) -> str:
        return self.extractor.name

    @property
    def dagster_name(self) -> str:
        """
        Generate a dagster safe name (^[A-Za-z0-9_]+$.)
        """
        return generate_dagster_name(self.name)

    @property
    def select_service(self) -> SelectService:
        return SelectService(
            project=self.meltano.project,
            extractor=self.name,
        )

    @property
    def streams(self):
        with closing(self.meltano.session()) as session:
            loop = asyncio.get_event_loop()
            streams = loop.run_until_complete(self.select_service.list_all(session)).streams

        return [stream for stream in streams if stream.selection != SelectionType.EXCLUDED]

    @property
    def asset(self) -> AssetsDefinition:
        outs = {
            stream.key: Out(
                dagster_type=Nothing,
                is_required=False,
                asset_key=AssetKey(
                    [self.dagster_name, stream.key],
                ),
            )
            for stream in self.streams
        }

        loader_name = "target-postgres"

        extract_load_op = extract_load_factory(
            name=f"{self.dagster_name}_{generate_dagster_name(loader_name)}",
            extractor_name=self.name,
            loader_name="target-postgres",
            outs=outs,
        )

        return multi_asset(
            name=self.dagster_name,
            ins={},
            outs=outs,
            can_subset=True,
            compute_kind="singer",
            group_name=self.dagster_name,
            required_resource_keys={"meltano"},
        )(extract_load_op)
