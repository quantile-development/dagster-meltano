from typing import List, Optional

from dagster import AssetsDefinition
from dagster_dbt import load_assets_from_dbt_project

# from dagster_meltano.meltano_resource import MeltanoResource
# from .utils import generate_dbt_group_name


def load_assets_from_meltano_project(
    meltano_project_dir: str,
    dbt_project_dir: Optional[str] = None,
    dbt_profiles_dir: Optional[str] = None,
    dbt_target_dir: Optional[str] = None,
    dbt_use_build_command: bool = True,
) -> List[AssetsDefinition]:
    """This function generates all Assets it can find in the supplied Meltano project.
    This currently includes the taps and dbt assets.

    Args:
        project_dir (Optional[str], optional): The location of the Meltano project. Defaults to os.getenv("MELTANO_PROJECT_ROOT").

    Returns:
        List[AssetsDefinition]: Returns a list of all Meltano assets
    """
    return []
    meltano_resource = MeltanoResource(meltano_project_dir)
    meltano_assets = [extractor.asset for extractor in meltano_resource.extractors]

    if dbt_project_dir:
        dbt_assets = load_assets_from_dbt_project(
            project_dir=dbt_project_dir,
            profiles_dir=dbt_profiles_dir,
            target_dir=dbt_target_dir,
            use_build_command=dbt_use_build_command,
            node_info_to_group_fn=generate_dbt_group_name,
        )
        meltano_assets += dbt_assets

    return meltano_assets
