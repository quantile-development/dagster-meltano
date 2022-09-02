from __future__ import annotations

import asyncio
from contextlib import closing
from typing import TYPE_CHECKING

from dagster import AssetKey, AssetsDefinition, Nothing, Out, multi_asset
from meltano.core.plugin import PluginDefinition
from meltano.core.plugin.singer.catalog import SelectionType
from meltano.core.select_service import SelectService

from ..utils import generate_dagster_name

if TYPE_CHECKING:
    from .resource import MeltanoResource

from .extract_load import extract_load_factory


class Extractor:
    def __init__(self, extractor: PluginDefinition, meltano: MeltanoResource):
        self.extractor = extractor
        self.meltano = meltano

    @property
    def name(self) -> str:
        return self.extractor.name

    @property
    def dagster_name(self) -> str:
        """
        Generate a dagster safe name (^[A-Za-z0-9_]+$.)
        """
        return generate_dagster_name(self.name)

    @property
    def select_service(self) -> SelectService:
        return SelectService(
            project=self.meltano.project,
            extractor=self.name,
        )

    @property
    def streams(self):
        with closing(self.meltano.session()) as session:
            loop = asyncio.get_event_loop()
            streams = loop.run_until_complete(self.select_service.list_all(session)).streams

        return [stream for stream in streams if stream.selection != SelectionType.EXCLUDED]

    @property
    def asset(self) -> AssetsDefinition:
        outs = {
            stream.key: Out(
                dagster_type=Nothing,
                is_required=False,
                asset_key=AssetKey(
                    [self.dagster_name, stream.key],
                ),
            )
            for stream in self.streams
        }

        loader_name = "target-postgres"

        extract_load_op = extract_load_factory(
            name=f"{self.dagster_name}_{generate_dagster_name(loader_name)}",
            extractor_name=self.name,
            loader_name="target-postgres",
            outs=outs,
        )

        return multi_asset(
            name=self.dagster_name,
            ins={},
            outs=outs,
            can_subset=True,
            compute_kind="singer",
            group_name=self.dagster_name,
            required_resource_keys={"meltano"},
        )(extract_load_op)
