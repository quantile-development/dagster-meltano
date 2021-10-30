from dagster import DagsterType, Permissive, dagster_type_loader


def _is_dict(elt_args: dict) -> bool:
    return isinstance(elt_args, dict)


@dagster_type_loader(config_schema={})
def load_empty_dict(_context, value):
    return {}


MeltanoEltArgsType = DagsterType(
    name="MeltanoEltArgs",
    type_check_fn=lambda _, elt_args: _is_dict(elt_args),
    description='Meltano elt arguments, you can define a "tap", "target" and "job_id" key.',
    typing_type=dict,
    loader=load_empty_dict,
)
