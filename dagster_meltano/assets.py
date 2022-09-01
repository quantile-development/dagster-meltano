import os
from functools import cache
from typing import List, Optional

from dagster import AssetsDefinition

from .meltano.resource import MeltanoResource


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
    meltano_resource = MeltanoResource(project_dir)
    return [extractor.asset for extractor in meltano_resource.extractors]
