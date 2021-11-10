from dagster import DagsterType, dagster_type_loader


def _is_dict(elt_args: dict) -> bool:
    return isinstance(elt_args, dict)


def _is_list_of_lists(patterns: list) -> bool:
    if len(patterns) == 0:
        return isinstance(patterns, list)
    else:
        for pattern in patterns:
            if not isinstance(pattern, list):
                return False

            for arg in pattern:
                if not isinstance(arg, str):
                    return False

    return True


@dagster_type_loader(config_schema={})
def load_empty_dict(_context, value):  # pylint: disable=unused-argument
    return {}


@dagster_type_loader(config_schema={})
def load_empty_list(_context, value):  # pylint: disable=unused-argument
    return []


MeltanoEltArgsType = DagsterType(
    name="MeltanoEltArgs",
    type_check_fn=lambda _, elt_args: _is_dict(elt_args),
    description='Meltano elt arguments, you can define a "tap", "target" and "job_id" key.',
    typing_type=dict,
    loader=load_empty_dict,
)

MeltanoSelectPatternsType = DagsterType(
    name="MeltanoSelectPatterns",
    type_check_fn=lambda _, select_patterns: _is_list_of_lists(select_patterns),
    description="""Meltano select patterns provided as a list of lists (e.g.,
    [['entity', 'attribute'], ['--rm', 'entity', 'attribute']]""",
    typing_type=list,
    loader=load_empty_list,
)

MeltanoEnvVarsType = DagsterType(
    name="MeltanoEnvVars",
    type_check_fn=lambda _, env_vars: _is_dict(env_vars),
    description="Injects environment variables into the meltano process.",
    typing_type=dict,
    loader=load_empty_dict,
)
