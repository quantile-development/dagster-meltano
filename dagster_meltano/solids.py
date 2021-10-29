"""Composable solids for configuring Meltano commands in pipeline."""

from types import FunctionType
from typing import Generator, List

from dagster import (
    AssetMaterialization,
    Field,
    InputDefinition,
    Nothing,
    Optional,
    OutputDefinition,
    SolidDefinition,
    SolidExecutionContext,
    check,
    solid,
)

from dagster_meltano.meltano_elt import MeltanoELT


def run_elt(
    name: str,
    tap: str,
    target: str,
    job_id: str,
    full_refresh: bool,
    env_vars: Optional[dict],
    log: SolidExecutionContext.log,
) -> Generator[AssetMaterialization, None, None]:
    """Run `meltano elt` command yielding asset materialization and producing logs.

    Args:
        name (str): The name of the solid.
        tap (str): The name of the Meltano tap.
        target (str): The name of the Meltano target.
        job_id (str): The id of the job.
        full_refresh (bool): Whether to ignore existing state.
        env_vars (Optional[dict]): Additional environment variables to pass to the
            command context.
        log (SolidExecutionContext.log): The solid execution context's logger.
    """
    meltano_elt = MeltanoELT(
        tap=tap,
        target=target,
        job_id=job_id,
        full_refresh=full_refresh,
        env_vars=env_vars,
    )

    for line in meltano_elt.logs:
        log.info(line)

    meltano_elt.elt_process.wait()

    return_code = meltano_elt.elt_process.returncode

    if return_code != 0:
        error = f"The meltano elt failed with code {return_code}"
        log.error(error)
        raise Exception(error)
    else:
        log.info(f"Meltano exited with return code {return_code}")

    yield AssetMaterialization(
        asset_key=name,
        metadata={
            "Tap": tap,
            "Target": target,
            "Job ID": job_id,
            "Full Refresh": "True" if full_refresh else "False",
        },
    )


def elt_factory(
    name: str, tap: str, target: str, job_id: str, env_vars: Optional[dict]
) -> FunctionType:
    """Produce solid command for defining solid at construction time."""
    check.str_param(name, "name")
    check.str_param(tap, "tap")
    check.str_param(target, "target")
    check.str_param(job_id, "job_id")
    check.dict_param(env_vars, "env_vars", key_type=str, value_type=str)

    def command(
        step_context: SolidExecutionContext, inputs  # pylint: disable=W0613
    ) -> Generator[AssetMaterialization, None, None]:
        check.inst_param(step_context, "step_context", SolidExecutionContext)
        check.param_invariant(
            isinstance(step_context.run_config, dict),
            "context",
            "StepExecutionContext must have valid run_config",
        )

        full_refresh = step_context.solid_config["full_refresh"]

        log = step_context.log

        return run_elt(name, tap, target, job_id, full_refresh, env_vars, log)

    return command


def optional_args(name, tap, target, job_id) -> (str, str):
    """Resolve optional Meltano elt args.

    Args:
        name (str): The name of the solid.
        tap (str): The name of the Meltano tap.
        target (str): The name of the Meltano target.
        job_id (str): The id of the job.
    """
    # If no name is specified, create a name based on the tap and target name
    if not name:
        name = f"{tap}_{target}"

    # Solid names cannot contain dashes
    name = name.replace("-", "_")

    # If no job id is defined, we base it on the tap and target name
    if not job_id:
        job_id = f"{tap}-{target}"

    return name, job_id


def meltano_elt_constructor(
    tap: str,
    target: str,
    input_defs: Optional[List[InputDefinition]] = None,
    output_defs: Optional[List[OutputDefinition]] = None,
    name: Optional[str] = None,
    job_id: Optional[str] = None,
    env_vars: Optional[dict] = None,
) -> SolidDefinition:
    """Create a solid for a meltano elt process.

    Note that you can only use `meltano_elt_constructor` if you know the command you'd like to execute
    at pipeline construction time. If you'd like to construct shell commands dynamically during
    pipeline execution and pass them between solids, you should use `meltano_elt_solid` instead.

    Args:
        tap (str): The name of the Meltano tap.
        target (str): The name of the Meltano target.
        input_defs (Optional[List[InputDefinition]]): Input definitions for the solid.
        output_defs (Optional[List[OutputDefinition]]): Output definitions for the solid.
        name (str): The name of the solid.
        job_id (str): The id of the job.
        env_vars (Optional[dict]): Additional environment variables to pass to the
            command context.

    Returns:
        SolidDefinition: The solid that runs the Meltano ELT process.
    """
    if input_defs is None:
        input_defs = []

    if output_defs is None:
        output_defs = []

    check.opt_str_param(name, "name")
    check.opt_str_param(job_id, "job_id")

    name, job_id = optional_args(name, tap, target, job_id)

    # Add a default tag to indicate this is a Meltano solid
    default_tags = {"kind": "meltano"}

    return SolidDefinition(
        name=name,
        input_defs=input_defs,
        output_defs=output_defs,
        compute_fn=elt_factory(
            name=name,
            tap=tap,
            target=target,
            job_id=job_id,
            env_vars=env_vars,
        ),
        config_schema={
            "full_refresh": Field(
                bool,
                default_value=False,
                description="Whether to ignore state on this run",
            )
        },
        required_resource_keys=set(),
        description="",
        tags={**default_tags},
    )()


@solid(
    name="meltano_elt_solid",
    description=(
        "This solid executes a Meltano elt command it receives as input.\n\n"
        "This solid is suitable for uses where the command to execute is generated dynamically by "
        "upstream solids. If you know the command to execute at pipeline construction time, consider"
        "`meltano_elt_constructor` instead."
    ),
    output_defs=[OutputDefinition(dagster_type=Nothing)],
    config_schema={
        "full_refresh": Field(
            bool,
            default_value=False,
            is_required=False,
            description="Whether to ignore existing state.",
        ),
        "env_vars": Field(
            dict,
            default_value={},
            is_required=False,
            description="Additional environment variables to pass to the shell command."
            "Overwrites conflicting variables passed from upstream solids/ops.",
        ),
    },
    tags={"kind": "meltano"},
)
def meltano_elt_solid(
    context, elt_args: dict, env_vars: Optional[dict] = None
) -> Generator[AssetMaterialization, None, None]:
    """This solid executes a Meltano elt command it receives as input.

    This solid is suitable for uses where the command to execute is generated dynamically by
    upstream solids. If you know the command to execute at pipeline construction time, consider
    `meltano_elt_constructor` instead.

    Args:
        context (SolidExecutionContext): Execution context for the solid
        elt_args (Dict[str: str]): Dictionary of arguments for a meltano elt command. Must
            include `tap` and `target`. Can include `name` and `job_id`.
        env_vars (dict): Additional environment variables to pass to the command context.
            Overwritten by conflicting variables passed in through config.
    """
    if env_vars is None:
        env_vars = {}

    # validate elt_args key names
    rqd_keys = ["tap", "target"]
    for k in rqd_keys:
        if not elt_args[k]:
            raise context.log.error(f"elt_args requires the '{k}' key")

    # resolve elt command args
    name = elt_args.get("name")
    tap = elt_args.get("tap")
    target = elt_args.get("target")
    job_id = elt_args.get("job_id")
    name, job_id = optional_args(name, tap, target, job_id)
    full_refresh = context.solid_config["full_refresh"]
    env_vars = {**env_vars, **context.solid_config["env_vars"]}

    # run command
    return run_elt(name, tap, target, job_id, full_refresh, env_vars, context.log)
