import logging
import os
from functools import lru_cache
from typing import Dict, List, Optional

from dagster import resource
from meltano.core.db import project_engine
from meltano.core.logging.utils import setup_logging
from meltano.core.plugin import PluginDefinition, PluginType
from meltano.core.project import Project
from meltano.core.project_plugins_service import ProjectPluginsService

from dagster_meltano.utils import Singleton

from .extractor import Extractor


class MeltanoResource(metaclass=Singleton):
    def __init__(self, project_dir: Optional[str] = os.getenv("MELTANO_PROJECT_ROOT")):
        self.project = Project(project_dir)
        setup_logging(self.project)

    @property
    @lru_cache
    def session(self):
        return project_engine(self.project)[1]

    @property
    def plugins_service(self) -> ProjectPluginsService:
        return ProjectPluginsService(self.project)

    @property
    def plugins(self) -> Dict[PluginType, List[PluginDefinition]]:
        return self.plugins_service.plugins_by_type()

    @property
    def extractors(self) -> List[Extractor]:
        return [Extractor(extractor, self) for extractor in self.plugins.get(PluginType.EXTRACTORS)]


@resource
def meltano_resource(init_context):
    logging.info("init_context =======================================")
    logging.info(init_context)
    return MeltanoResource()
