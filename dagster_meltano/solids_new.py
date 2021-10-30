from typing import Any, Dict

from dagster import (
    Field,
    InputDefinition,
    Nothing,
    Optional,
    OutputDefinition,
    SolidDefinition,
    SolidExecutionContext,
)

from dagster_meltano.dagster_types import MeltanoEltArgsType
from dagster_meltano.meltano_elt import MeltanoELT


class MeltanoEltSolid:
    input_defs = [
        InputDefinition("before", dagster_type=Nothing),
        InputDefinition(
            "elt_args",
            dagster_type=Optional[MeltanoEltArgsType],
            default_value={},
            description="""
            Use this type to overwrite meltano elt commands at runtime. 
            You can supply the 'tap', 'target' and 'job_id' keys in a dictionary.
            """,
        ),
    ]
    output_defs = [OutputDefinition(dagster_type=Nothing, name="after")]

    def __init__(self, name: str, tap: str = None, target: str = None, job_id: str = None) -> None:
        self.name = name
        self.tap = tap
        self.target = target
        self.job_id = job_id

    @property
    def _config_schema(self):
        return {
            "tap": Field(
                str,
                is_required=False,
                description="The singer tap that extracts the data.",
            ),
            "target": Field(
                str,
                is_required=False,
                description="The singer target that stores the data.",
            ),
            "job_id": Field(
                str,
                is_required=False,
                description="The meltano job id, used for incremental replication.",
            ),
        }

    def _compute_hook(self, step_context: SolidExecutionContext, inputs: Dict[str, Any]):
        elt_args = inputs['elt_args']

        upstream_tap = elt_args.get("tap")
        upstream_target = elt_args.get("target")
        upstream_job_id = elt_args.get("job_id")

        self_tap = self.tap
        self_target = self.target
        self_job_id = self.job_id

        config_tap = step_context.solid_config.get("tap")
        config_target = step_context.solid_config.get("target")
        config_job_id = step_context.solid_config.get("job_id")

        tap = upstream_tap or config_tap or self_tap
        target = upstream_target or config_target or self_target
        job_id = upstream_job_id or config_job_id or self_job_id

        meltano_elt = MeltanoELT(
            tap=tap,
            target=target,
            job_id=job_id,
            full_refresh=False,
            # env_vars=env_vars,
        )

        meltano_elt.run(log=step_context.log)

    @property
    def solid(self) -> SolidDefinition:
        return SolidDefinition(
            name=self.name,
            input_defs=self.input_defs,
            output_defs=self.output_defs,
            required_resource_keys=set(),
            config_schema=self._config_schema,
            compute_fn=self._compute_hook,
            tags={"kind": "meltano"},
        )
