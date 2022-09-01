from typing import Any, Dict, List

from dagster import (
    AssetMaterialization,
    Field,
    In,
    InputDefinition,
    Nothing,
    OpExecutionContext,
    Optional,
    Out,
    OutputDefinition,
    Permissive,
    SolidDefinition,
    SolidExecutionContext,
    op,
)

from dagster_meltano.dagster_types import MeltanoEltArgsType, MeltanoEnvVarsType
from dagster_meltano.meltano_elt import MeltanoELT


@op(
    ins={
        "after": In(Nothing),
    },
    out={
        "before": Out(Nothing),
    },
    config_schema={
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
        "env_vars": Field(
            Permissive({}),
            is_required=False,
            description="Inject custom environment variables into the Meltano elt process.",
        ),
        "tap_config": Field(
            Permissive({}),
            is_required=False,
            description="Overwrite the tap configuration.",
        ),
        "target_config": Field(
            Permissive({}),
            is_required=False,
            description="Overwrite the target configuration.",
        ),
        "full_refresh": Field(
            bool,
            is_required=False,
            description="Whether to overwrite all existing data or not.",
        ),
    },
)
def meltano_op(context: OpExecutionContext):
    print("hello world")


class MeltanoEltSolid:
    input_defs = [
        InputDefinition(
            "before",
            dagster_type=Nothing,
            description="Use this input to run the Meltano elt solid after another solid.",
        ),
        InputDefinition(
            "elt_args",
            dagster_type=Optional[MeltanoEltArgsType],
            default_value={},
            description="""
            Use this type to overwrite Meltano elt commands at runtime. 
            You can supply the 'tap', 'target' and 'job_id' keys in a dictionary.
            """,
        ),
        InputDefinition(
            "env_vars",
            dagster_type=Optional[MeltanoEnvVarsType],
            default_value={},
            description="""
            Use this type to inject environment variables into the Meltano elt process.
            """,
        ),
    ]
    output_defs = [OutputDefinition(dagster_type=Nothing, name="after")]
    config_schema = {
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
        "env_vars": Field(
            Permissive({}),
            is_required=False,
            description="Inject custom environment variables into the Meltano elt process.",
        ),
        "tap_config": Field(
            Permissive({}),
            is_required=False,
            description="Overwrite the tap configuration.",
        ),
        "target_config": Field(
            Permissive({}),
            is_required=False,
            description="Overwrite the target configuration.",
        ),
        "full_refresh": Field(
            bool,
            is_required=False,
            description="Whether to overwrite all existing data or not.",
        ),
    }

    def __init__(
        self,
        name: str,
        tap: Optional[str] = None,
        target: Optional[str] = None,
        job_id: Optional[str] = None,
        tap_config: Optional[dict] = None,
        target_config: Optional[dict] = None,
        env_vars: Optional[dict] = None,
        full_refresh: Optional[bool] = False,
    ) -> None:
        if tap_config == None:
            self.tap_config = {}
        if target_config == None:
            self.target_config = {}
        if env_vars == None:
            self.env_vars = {}

        self.name = name
        self.tap = tap
        self.target = target
        self.job_id = job_id
        self.tap_config = tap_config
        self.target_config = target_config
        self.env_vars = env_vars
        self.full_refresh = full_refresh

    def _compute_hook(self, step_context: SolidExecutionContext, inputs: Dict[str, Any]):
        # Fetch all variables from upstream nodes
        elt_args = inputs["elt_args"]
        upstream_tap = elt_args.get("tap")
        upstream_target = elt_args.get("target")
        upstream_job_id = elt_args.get("job_id")
        upstream_env_vars = inputs["env_vars"]

        # Fetch all variables defined in the initializer
        self_tap = self.tap
        self_target = self.target
        self_job_id = self.job_id
        self_tap_config = self.tap_config
        self_target_config = self.target_config
        self_env_vars = self.env_vars
        self_full_refresh = self.full_refresh

        # Fetch all variables defined by solid configuration
        config_tap = step_context.solid_config.get("tap")
        config_target = step_context.solid_config.get("target")
        config_job_id = step_context.solid_config.get("job_id")
        config_tap_config = step_context.solid_config.get("tap_config")
        config_target_config = step_context.solid_config.get("target_config")
        config_env_vars = step_context.solid_config.get("env_vars")
        config_full_refresh = step_context.solid_config.get("full_refresh")

        # Define the final variables (upstream > dagster-config > intializer)
        tap = upstream_tap or config_tap or self_tap
        target = upstream_target or config_target or self_target
        job_id = (
            upstream_job_id or config_job_id or self_job_id or self.construct_job_id(tap, target)
        )
        tap_config = config_tap_config or self_tap_config
        target_config = config_target_config or self_target_config
        env_vars = upstream_env_vars or config_env_vars or self_env_vars
        full_refresh = config_full_refresh or self_full_refresh

        meltano_elt_args = {
            "tap": tap,
            "target": target,
            "job_id": job_id,
            "full_refresh": full_refresh,
            "tap_config": tap_config,
            "target_config": target_config,
            "env_vars": env_vars,
        }

        # Create the Meltano elt process
        meltano_elt = MeltanoELT(**meltano_elt_args)

        # Run the elt process
        meltano_elt.run(log=step_context.log)

        # Yield the asset materialization that show information about the elt process
        yield self.asset_materialization(
            asset_key=self.construct_asset_key(step_context=step_context), **meltano_elt_args
        )

    @staticmethod
    def asset_materialization(
        asset_key: str,
        tap: str,
        target: str,
        job_id: str,
        full_refresh: bool,
        tap_config: Optional[dict],
        target_config: Optional[dict],
        env_vars: Optional[dict],
    ) -> AssetMaterialization:
        metadata = {
            "tap": tap,
            "target": target,
            "job-id": job_id,
            "full-refresh": 1 if full_refresh else 0,
        }

        if tap_config:
            metadata["tap-config"] = tap_config
        if target_config:
            metadata["target-config"] = target_config
        if env_vars:
            metadata["env-vars"] = env_vars

        return AssetMaterialization(
            asset_key=asset_key,
            metadata=metadata,
        )

    @staticmethod
    def construct_asset_key(step_context: SolidExecutionContext) -> List[str]:
        return [step_context.pipeline_name, step_context.solid_def.name]

    @staticmethod
    def construct_job_id(tap: str, target: str) -> str:
        return f"{tap}-{target}"

    @property
    def solid(self) -> SolidDefinition:
        return SolidDefinition(
            name=self.name,
            input_defs=self.input_defs,
            output_defs=self.output_defs,
            description="Executes the Meltano elt process.",
            required_resource_keys=set(),
            config_schema=self.config_schema,
            compute_fn=self._compute_hook,
            tags={"kind": "meltano"},
        )
