from dagster._core.definitions.asset_key import AssetKey
from dagster._core.definitions.asset_selection import AssetSelection
from dagster._core.definitions.assets import AssetsDefinition
from dagster._core.definitions.cacheable_assets import (
    AssetsDefinitionCacheableData,
    CacheableAssetsDefinition,
)
from dagster._core.definitions.decorators.asset_decorator import asset
from dagster._core.definitions.decorators.op_decorator import op
from dagster._core.definitions.decorators.repository_decorator import repository
from dagster._core.definitions.unresolved_asset_job_definition import define_asset_job

from dagster_tests.general_tests.test_repository import (
    define_empty_job,
    define_simple_job,
    define_with_resources_job,
)


def define_cacheable_and_uncacheable_assets():
    class MyCacheableAssets(CacheableAssetsDefinition):
        def compute_cacheable_data(self):
            return [
                AssetsDefinitionCacheableData(
                    keys_by_input_name={"upstream": AssetKey("upstream")},
                    keys_by_output_name={"result": AssetKey(self.unique_id)},
                )
            ]

        def build_definitions(self, data):
            @op(name=self.unique_id)
            def _op(upstream):
                return upstream + 1

            return [
                AssetsDefinition.from_op(
                    _op,
                    keys_by_input_name=cd.keys_by_input_name,
                    keys_by_output_name=cd.keys_by_output_name,
                )
                for cd in data
            ]

    @asset
    def upstream():
        return 1

    @asset
    def downstream(a, b):
        return a + b

    return [MyCacheableAssets("a"), MyCacheableAssets("b"), upstream, downstream]


@repository
def cacheable_asset_repo():
    return [
        define_empty_job(),
        define_simple_job(),
        *define_with_resources_job(),
        define_cacheable_and_uncacheable_assets(),
        define_asset_job(
            "all_asset_job",
            selection=AssetSelection.assets(
                AssetKey("a"), AssetKey("b"), AssetKey("upstream"), AssetKey("downstream")
            ),
        ),
    ]
