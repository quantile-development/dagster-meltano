from __future__ import annotations

import asyncio
import logging
from contextlib import closing
from typing import TYPE_CHECKING, Dict, Optional

from dagster import Field, OpExecutionContext, Out, Output, op
from meltano.cli.elt import _elt_context_builder, _run_extract_load
from meltano.core.job import Job
from meltano.core.logging import JobLoggingService, OutputLogger

if TYPE_CHECKING:
    from .resource import MeltanoResource

from dataclasses import dataclass

from .logger import add_repeat_logger


@dataclass
class Config:
    full_refresh: str = False
    state_suffix: Optional[str] = None
    state_id: Optional[str] = None


def generate_state_id(
    environment: str,
    extractor_name: str,
    loader_name: str,
    suffix: str,
):
    state_id = f"{environment}:{extractor_name}-to-{loader_name}"

    if suffix:
        state_id += f":{suffix}"

    return state_id


def load_config(context: OpExecutionContext) -> Config:
    """
    If no config was provided the `op_config` value is None.
    In this case we create a Config instance with default values.
    """
    if context.op_config == None:
        return Config()

    full_refresh = context.op_config.get("full_refresh", False)
    state_suffix = context.op_config.get("state_suffix", None)
    state_id = context.op_config.get("state_id", None)

    return Config(
        full_refresh=full_refresh,
        state_suffix=state_suffix,
        state_id=state_id,
    )


def extract_load_factory(
    name: str,
    extractor_name: str,
    loader_name: str,
    outs: Dict[str, Out],
):
    @op(
        name=name,
        out=outs,
        required_resource_keys={"meltano"},
        tags={"kind": "singer"},
        config_schema={
            "full_refresh": Field(
                bool,
                default_value=False,
                description="Whether to ignore existing state.",
            ),
            "state_id": Field(
                str,
                is_required=False,
                description="Use this field to overwrite the default generated state_id.",
            ),
            "state_suffix": Field(
                str,
                is_required=False,
                description="This will be appended to the Meltano state id. Ignored if `state_id` provided.",
            ),
        },
    )
    def extract_load(context: OpExecutionContext):
        log = context.log
        meltano_resource: MeltanoResource = context.resources.meltano
        environment = meltano_resource.environment

        log.info(context)

        config = load_config(context)

        if config.state_id == None:
            config.state_id = generate_state_id(
                environment,
                extractor_name,
                loader_name,
                config.state_suffix,
            )

        add_repeat_logger(
            logger=logging.getLogger("meltano"),
            dagster_logger=log,
        )

        select_filter = list(context.selected_output_names)

        job = Job(job_name=config.state_id)

        with closing(meltano_resource.session()) as session:
            plugins_service = meltano_resource.plugins_service
            context_builder = _elt_context_builder(
                project=meltano_resource.project,
                job=job,
                session=session,
                extractor=extractor_name,
                loader=loader_name,
                transform="skip",
                full_refresh=config.full_refresh,
                select_filter=select_filter,
                plugins_service=plugins_service,
            ).context()

            job_logging_service = JobLoggingService(meltano_resource.project)

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

    return extract_load
