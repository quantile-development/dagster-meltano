from __future__ import annotations
from functools import lru_cache
from typing import TYPE_CHECKING, Optional
from dagster import (
    In,
    Nothing,
    OpDefinition,
    Out,
    get_dagster_logger,
    op,
    Field,
)

from dagster_meltano.utils import generate_dagster_name

if TYPE_CHECKING:
    from dagster_meltano.meltano_resource import MeltanoResource

dagster_logger = get_dagster_logger()

STDOUT = 1


@lru_cache
def meltano_command_op(
    command: str,
    dagster_name: Optional[str] = None,
) -> OpDefinition:
    """
    Run `meltano <command>` using a Dagster op.

    This factory is cached to make sure the same commands can be reused in the
    same repository.

    Args:
        command (str): The Meltano command to run.
        dagster_name (Optional[str], optional): The Dagster name to use for the op.
            Defaults to None.

    Returns:
        OpDefinition: The Dagster op definition.
    """
    dagster_name = dagster_name or generate_dagster_name(command)
    ins = {
        "after": In(Nothing),
        "env": In(
            Optional[dict],
            description="Environment variables to inject into the Meltano process.",
            default_value={},
        ),
    }
    out = {
        "logs": Out(str, description="The output logs of the Meltano command."),
    }

    @op(
        name=dagster_name,
        description=f"Run `meltano {command}`.",
        ins=ins,
        out=out,
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
    def dagster_op(
        context,
        env: Optional[dict],
    ) -> str:
        """
        Run `meltano run <command>` using a Dagster op.

        Args:
            context: The Dagster op execution context.
        """
        meltano_resource: MeltanoResource = context.resources.meltano

        # Get the environment variables from the config and
        # add them to the Meltano invoker
        config_env = context.op_config.get("env")

        # The environment variables are a combination of the ones from the
        # Dagster op input and the ones from the config
        env = {**env, **config_env}

        # Run the Meltano command
        output = meltano_resource.execute_command(f"{command}", env, context.log)

        # Return the logs
        return output

    return dagster_op


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
    return meltano_command_op(
        command=f"run {command} --force", dagster_name=dagster_name
    )


@op(
    name=generate_dagster_name("meltano install"),
    description="Install all Meltano plugins",
    ins={"after": In(Nothing)},
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
def meltano_install_op(context):
    """
    Run `meltano install` using a Dagster op.
    """
    meltano_resource: MeltanoResource = context.resources.meltano

    # Get the environment variables from the config and
    # add them to the Meltano invoker
    config_env = context.op_config.get("env")

    # Run the Meltano run command
    meltano_resource.execute_command(f"install", config_env, context.log)
