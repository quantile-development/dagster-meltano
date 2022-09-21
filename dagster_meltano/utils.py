class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def generate_dagster_name(string) -> str:
    """
    Generate a dagster safe name (^[A-Za-z0-9_]+$.)
    """
    return string.replace("-", "_").replace(" ", "_")


def generate_dbt_group_name(node_info):
    if len(node_info.get("fqn", [])) >= 3:
        return "_".join(node_info["fqn"][1:-1])

    return "dbt"
