from __future__ import annotations
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Dict, Optional
import json
from dagster import (
    In,
    Nothing,
    OpDefinition,
    OpExecutionContext,
    get_dagster_logger,
    op,
    Field,
)

from dagster_meltano.log_processing.metadata_processor import MetadataLogProcessor
from dagster_meltano.utils import generate_dagster_name

if TYPE_CHECKING:
    from dagster_meltano.meltano_resource import MeltanoResource

dagster_logger = get_dagster_logger()

STDOUT = 1


@lru_cache
def meltano_run_op(
    command: str,
) -> OpDefinition:
    """
    Run `meltano run <command>` using a Dagster op.

    This factory is cached to make sure the same commands can be reused in the
    same repository.
    """
    dagster_name = generate_dagster_name(command)
    ins = {
        "after": In(Nothing),
    }

    @op(
        name=dagster_name,
        description=f"Run `{command}` using Meltano.",
        ins=ins,
        tags={"kind": "meltano"},
        required_resource_keys={"meltano"},
        config_schema={
            "env": Field(
                dict,
                description="Environment variables to inject into the Meltano run process.",
                default_value={},
                is_required=False,
            )
        },
    )
    def dagster_op(context: OpExecutionContext):
        """
        Run `meltano run <command>` using a Dagster op.

        Args:
            context (OpExecutionContext): The Dagster op execution context.
        """
        meltano_resource: MeltanoResource = context.resources.meltano

        # Get the environment variables from the config and
        # add them to the Meltano invoker
        env = context.op_config.get("env")
        meltano_resource.meltano_invoker.env.update(env)

        _log_results = meltano_resource.meltano_invoker.run_and_log(
            "run",
            MetadataLogProcessor,
            command.split(),
        )

    return dagster_op


@op(
    name=generate_dagster_name("meltano install"),
    description="Install all Meltano plugins",
    ins={"after": In(Nothing)},
    tags={"kind": "meltano"},
    required_resource_keys={"meltano"},
)
def meltano_install_op(context: OpExecutionContext):
    """
    Run `meltano install` using a Dagster op.
    """
    meltano_resource: MeltanoResource = context.resources.meltano
    meltano_resource.meltano_invoker.run_and_log(
        "install",
        MetadataLogProcessor,
    )
