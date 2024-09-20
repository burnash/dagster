from dagster import (
    AssetKey,
    AssetsDefinition,
    DagsterInstance,
    asset,
    define_asset_job,
    op,
    repository,
    with_resources,
)
from dagster._core.definitions.cacheable_assets import (
    AssetsDefinitionCacheableData,
    CacheableAssetsDefinition,
)
from dagster._core.execution.plan.external_step import local_external_step_launcher


class MyCacheableAssetsDefinition(CacheableAssetsDefinition):
    _cacheable_data = AssetsDefinitionCacheableData(keys_by_output_name={"result": AssetKey("foo")})

    def compute_cacheable_data(self):
        # used for tracking how many times this function gets called over an execution
        # since we're crossing process boundaries, we pre-populate this value in the host process
        # and assert that this pre-populated value is present, to ensure that we'll error if this
        # gets called in a child process
        instance = DagsterInstance.get()
        val = instance.run_storage.get_cursor_values({"val"}).get("val")
        assert val == "INITIAL_VALUE"
        instance.run_storage.set_cursor_values({"val": "NEW_VALUE"})
        return [self._cacheable_data]

    def build_definitions(self, data):
        assert len(data) == 1
        assert data == [self._cacheable_data]

        @op(required_resource_keys={"step_launcher"})
        def _op():
            return 1

        return with_resources(
            [
                AssetsDefinition.from_op(
                    _op,
                    keys_by_output_name=cd.keys_by_output_name,
                )
                for cd in data
            ],
            {"step_launcher": local_external_step_launcher},
        )


@asset
def bar(foo):
    return foo + 1


@repository
def cacheable_asset_repo():
    return [bar, MyCacheableAssetsDefinition("xyz"), define_asset_job("all_asset_job")]
