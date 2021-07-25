from types import FunctionType
from typing import Any, Generator, List
from dagster import (
    AssetMaterialization,
    InputDefinition, 
    OutputDefinition, 
    Optional,
    SolidDefinition, 
    SolidExecutionContext, 
    check,
    Nothing,
    Field
)
from dagster_meltano.meltano_elt import MeltanoELT

def run_elt(name: str, tap: str, target: str, job_id: str) -> FunctionType:
    check.str_param(name, 'name')
    check.str_param(tap, 'tap')
    check.str_param(target, 'target')
    check.str_param(job_id, 'job_id')

    def command(step_context: SolidExecutionContext, inputs) -> Generator[AssetMaterialization, None, None]:
        check.inst_param(step_context, "step_context", SolidExecutionContext)
        check.param_invariant(
            isinstance(step_context.run_config, dict),
            "context",
            "StepExecutionContext must have valid run_config",
        )

        full_refresh = step_context.solid_config['full_refresh']

        log = step_context.log

        meltano_elt = MeltanoELT(
            tap=tap,
            target=target,
            job_id=job_id,
            full_refresh=full_refresh
        )

        for line in meltano_elt.logs:
            log.info(line)

        meltano_elt.elt_process.wait()

        return_code = meltano_elt.elt_process.returncode

        if return_code != 0:
            error = f'The meltano elt failed with code {return_code}'
            log.error(error)
            raise Exception(error)
        else:
            log.info(
                f"Meltano exited with return code {return_code}"
            )

        yield AssetMaterialization(
            asset_key=name,
            metadata={
                'Tap': tap,
                'Target': target,
                'Job ID': job_id,
                'Full Refresh': 'True' if full_refresh else 'False'
            }
        )

    return command

def meltano_elt_solid(
    tap: str,
    target: str,
    input_defs: List[InputDefinition] = [],
    output_defs: List[OutputDefinition] = [],
    name: Optional[str] = None,
    job_id: Optional[str] = None
) -> SolidDefinition:
    """Create a solid for a meltano elt process.
    
    Args:
        name (str): The name of the solid.
        description (Optional[str]): If set, description used for solid.

    Returns:
        SolidDefinition: The solid that runs the Meltano ELT process.
    """
    check.opt_str_param(name, 'name')
    check.opt_str_param(job_id, 'job_id')

    # If no name is specified, create a name based on the tap and target name
    if not name:
        name = f'{tap}_{target}'
    
    # Solid names cannot contain dashes
    name = name.replace('-', '_')

    # If no job id is defined, we base it on the tap and target name
    if not job_id:
        job_id = f'{tap}-{target}'

    # Add a default tag to indicate this is a Meltano solid
    default_tags = {'kind': 'meltano'}

    return SolidDefinition(
        name=name,
        input_defs=input_defs,
        output_defs=output_defs,
        compute_fn=run_elt(
            name=name,
            tap=tap,
            target=target,
            job_id=job_id
        ),
        config_schema={
            'full_refresh': Field(bool, default_value=False, description='Whether to ignore state on this run')
        },
        required_resource_keys=set(),
        description='',
        tags={**default_tags},
    )()