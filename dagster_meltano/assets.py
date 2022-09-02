import os
from functools import cache
from typing import List, Optional

from dagster import AssetsDefinition
from dagster._utils import file_relative_path

from dagster_dbt import (
    dbt_cli_resource,
    dbt_docs_generate_op,
    dbt_ls_op,
    load_assets_from_dbt_project,
)

from .meltano.resource import MeltanoResource


def node_group_name(node_info):
    if len(node_info.get("fqn", [])) >= 3:
        return "_".join(node_info["fqn"][1:-1])

    return "dbt"


@cache
def load_assets_from_meltano_project(
    project_dir: Optional[str],
) -> List[AssetsDefinition]:
    """This function generates all Assets it can find in the supplied Meltano project.
    This currently includes the taps and dbt assets.

    Args:
        project_dir (Optional[str], optional): The location of the Meltano project. Defaults to os.getenv("MELTANO_PROJECT_ROOT").

    Returns:
        List[AssetsDefinition]: Returns a list of all Meltano assets
    """
    dbt_assets = load_assets_from_dbt_project(
        "/workspace/meltano/transform",
        "/workspace/meltano/transform/profiles",
        # target_dir=DBT_TARGET_PATH,
        node_info_to_group_fn=node_group_name,
    )
    meltano_resource = MeltanoResource(project_dir)
    return [extractor.asset for extractor in meltano_resource.extractors] + dbt_assets
