from typing import Sequence

from dagster import AssetsDefinition, asset, op, repository
from dagster._core.definitions.asset_key import AssetKey
from dagster._core.definitions.cacheable_assets import (
    AssetsDefinitionCacheableData,
    CacheableAssetsDefinition,
)
from dagster._core.definitions.metadata.metadata_value import MetadataValue
from dagster._core.definitions.metadata.table import TableColumn, TableSchema
from dagster._core.definitions.repository_definition.valid_definitions import (
    PendingRepositoryListDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import define_asset_job
from dagster._core.instance import DagsterInstance


class MyCacheableAssetsDefinition(CacheableAssetsDefinition):
    _cacheable_data = AssetsDefinitionCacheableData(
        keys_by_output_name={"result": AssetKey("foo")},
        metadata_by_output_name={
            "result": {
                "some_val": MetadataValue.table_schema(
                    schema=TableSchema(columns=[TableColumn("some_col")])
                )
            }
        },
    )

    def compute_cacheable_data(self):
        # used for tracking how many times this function gets called over an execution
        instance = DagsterInstance.get()
        kvs_key = "compute_cacheable_data_called"
        compute_cacheable_data_called = int(
            instance.run_storage.get_cursor_values({kvs_key}).get(kvs_key, "0")
        )
        instance.run_storage.set_cursor_values({kvs_key: str(compute_cacheable_data_called + 1)})
        # Skip the tracking if this is called outside the context of a DagsterInstance
        return [self._cacheable_data]

    def build_definitions(self, data):
        assert len(data) == 1
        assert data == [self._cacheable_data]

        # used for tracking how many times this function gets called over an execution
        instance = DagsterInstance.get()
        kvs_key = "get_definitions_called"
        get_definitions_called = int(
            instance.run_storage.get_cursor_values({kvs_key}).get(kvs_key, "0")
        )
        instance.run_storage.set_cursor_values({kvs_key: str(get_definitions_called + 1)})

        @op
        def _op():
            return 1

        return [
            AssetsDefinition.from_op(_op, keys_by_output_name=cd.keys_by_output_name) for cd in data
        ]


@asset
def bar(foo):
    return foo + 1


@repository
def cacheable_assets_repo() -> Sequence[PendingRepositoryListDefinition]:
    return [bar, MyCacheableAssetsDefinition("xyz"), define_asset_job("all_asset_job")]
