from dagster import AssetKey, AssetsDefinition, asset, define_asset_job, op, repository
from dagster._core.definitions.cacheable_assets import (
    AssetsDefinitionCacheableData,
    CacheableAssetsDefinition,
)
from dagster._core.instance import DagsterInstance

from dagster_tests.execution_tests.engine_tests.test_step_delegating_executor import (
    test_step_delegating_executor,
)


class MyCacheableAssetsDefinition(CacheableAssetsDefinition):
    _cacheable_data = AssetsDefinitionCacheableData(keys_by_output_name={"result": AssetKey("foo")})

    def compute_cacheable_data(self):
        # used for tracking how many times this function gets called over an execution
        instance = DagsterInstance.get()
        kvs_key = "compute_cacheable_data_called"
        num_called = int(instance.run_storage.get_cursor_values({kvs_key}).get(kvs_key, "0"))
        instance.run_storage.set_cursor_values({kvs_key: str(num_called + 1)})
        return [self._cacheable_data]

    def build_definitions(self, data):
        assert len(data) == 1
        assert data == [self._cacheable_data]
        # used for tracking how many times this function gets called over an execution
        instance = DagsterInstance.get()
        kvs_key = "get_definitions_called"
        num_called = int(instance.run_storage.get_cursor_values({kvs_key}).get(kvs_key, "0"))
        instance.run_storage.set_cursor_values({kvs_key: str(num_called + 1)})

        @op
        def _op():
            return 1

        return [
            AssetsDefinition.from_op(_op, keys_by_output_name=cd.keys_by_output_name) for cd in data
        ]


@asset
def bar(foo):
    return foo + 1


@repository(default_executor_def=test_step_delegating_executor)
def cacheable_asset_repo():
    return [bar, MyCacheableAssetsDefinition("xyz"), define_asset_job("all_asset_job")]
