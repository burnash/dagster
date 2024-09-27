import sys

import dagster._check as check
import pytest
from dagster import (
    AssetKey,
    AssetsDefinition,
    DagsterInvalidDefinitionError,
    JobDefinition,
    RepositoryDefinition,
    asset,
    define_asset_job,
    op,
    repository,
)
from dagster._core.code_pointer import CodePointer
from dagster._core.definitions.asset_selection import AssetSelection
from dagster._core.definitions.cacheable_assets import (
    AssetsDefinitionCacheableData,
    CacheableAssetsDefinition,
)
from dagster._core.definitions.definitions_load_context import DefinitionsLoadType
from dagster._core.definitions.reconstruct import repository_def_from_pointer
from dagster._core.definitions.repository_definition import RepositoryLoadData
from dagster._core.definitions.resource_definition import resource
from dagster._core.execution.with_resources import with_resources
from dagster._utils import file_relative_path
from dagster._utils.test import temp_definitions_load_context

cacheable_asset_repo_path = file_relative_path(__file__, "cacheable_asset_repo.py")
cacheable_asset_repo_pointer = CodePointer.from_python_file(
    python_file=cacheable_asset_repo_path,
    definition="cacheable_asset_repo",
    working_directory=None,
)


@pytest.fixture
def clear_cacheable_asset_repo_from_module_cache():
    if "cacheable_asset_repo" in sys.modules:
        del sys.modules["cacheable_asset_repo"]


@pytest.mark.usefixtures("clear_cacheable_asset_repo_from_module_cache")
def test_resolve_empty():
    repo = repository_def_from_pointer(
        cacheable_asset_repo_pointer, DefinitionsLoadType.INITIALIZATION
    )
    assert isinstance(repo, RepositoryDefinition)
    assert isinstance(repo.get_job("simple_job"), JobDefinition)
    assert isinstance(repo.get_job("all_asset_job"), JobDefinition)


@pytest.mark.usefixtures("clear_cacheable_asset_repo_from_module_cache")
def test_resolve_missing_key():
    with pytest.raises(check.CheckError, match="No metadata found"):
        repository_def_from_pointer(
            cacheable_asset_repo_pointer,
            DefinitionsLoadType.RECONSTRUCTION,
            repository_load_data=RepositoryLoadData(
                cacheable_asset_data={
                    "a": [
                        AssetsDefinitionCacheableData(
                            keys_by_input_name={"upstream": AssetKey("upstream")},
                            keys_by_output_name={"result": AssetKey("a")},
                        )
                    ]
                }
            ),
        )


@pytest.mark.usefixtures("clear_cacheable_asset_repo_from_module_cache")
def test_resolve_wrong_data():
    with pytest.raises(
        DagsterInvalidDefinitionError,
        match=(
            r"Input asset .*\"b\".* is not produced by any of the provided asset ops and is not one"
            r" of the provided sources"
        ),
    ):
        repository_def_from_pointer(
            cacheable_asset_repo_pointer,
            DefinitionsLoadType.RECONSTRUCTION,
            repository_load_data=RepositoryLoadData(
                cacheable_asset_data={
                    "a": [
                        AssetsDefinitionCacheableData(
                            keys_by_input_name={"upstream": AssetKey("upstream")},
                            keys_by_output_name={"result": AssetKey("a")},
                        )
                    ],
                    "b": [
                        AssetsDefinitionCacheableData(
                            keys_by_input_name={"upstream": AssetKey("upstream")},
                            keys_by_output_name={"result": AssetKey("BAD_ASSET_KEY")},
                        )
                    ],
                }
            ),
        )


def define_uncacheable_and_resource_dependent_cacheable_assets():
    class ResourceDependentCacheableAsset(CacheableAssetsDefinition):
        def __init__(self):
            super().__init__("res_midstream")

        def compute_cacheable_data(self):
            return [
                AssetsDefinitionCacheableData(
                    keys_by_input_name={"res_upstream": AssetKey("res_upstream")},
                    keys_by_output_name={"result": AssetKey("res_midstream")},
                )
            ]

        def build_definitions(self, data):
            @op(name="res_midstream", required_resource_keys={"foo"})
            def _op(context, res_upstream):
                return res_upstream + context.resources.foo

            return [
                AssetsDefinition.from_op(
                    _op,
                    keys_by_input_name=cd.keys_by_input_name,
                    keys_by_output_name=cd.keys_by_output_name,
                )
                for cd in data
            ]

    @asset
    def res_upstream(context):
        return context.resources.foo

    @asset
    def res_downstream(context, res_midstream):
        return res_midstream + context.resources.foo

    return [ResourceDependentCacheableAsset(), res_upstream, res_downstream]


def test_resolve_no_resources():
    """Test that loading a repo with a resource-dependent cacheable asset fails if the resource is not
    provided.
    """
    with temp_definitions_load_context():
        with pytest.raises(DagsterInvalidDefinitionError):
            try:

                @repository
                def resource_dependent_repo_no_resources():
                    return [
                        define_uncacheable_and_resource_dependent_cacheable_assets(),
                        define_asset_job(
                            "all_asset_job",
                        ),
                    ]

                resource_dependent_repo_no_resources.get_all_jobs()
            except DagsterInvalidDefinitionError as e:
                # Make sure we get an error for the cacheable asset in particular
                assert "res_midstream" in str(e)
                raise e


def test_resolve_with_resources():
    """Test that loading a repo with a resource-dependent cacheable asset succeeds if the resource is
    provided.
    """
    with temp_definitions_load_context():

        @resource
        def foo_resource():
            return 3

        @repository
        def resource_dependent_repo_with_resources():
            return [
                with_resources(
                    define_uncacheable_and_resource_dependent_cacheable_assets(),
                    {"foo": foo_resource},
                ),
                define_asset_job(
                    "all_asset_job",
                ),
            ]

        assert isinstance(resource_dependent_repo_with_resources, RepositoryDefinition)
        assert isinstance(
            resource_dependent_repo_with_resources.get_job("all_asset_job"), JobDefinition
        )


def test_group_cached_assets():
    """Test that with_attributes works properly on cacheable assets."""

    class MyCacheableAssets(CacheableAssetsDefinition):
        def compute_cacheable_data(self):
            return [
                AssetsDefinitionCacheableData(
                    keys_by_input_name={}, keys_by_output_name={"result": AssetKey(self.unique_id)}
                )
            ]

        def build_definitions(self, data):
            @op(name=self.unique_id)
            def _op():
                return 5

            return [
                AssetsDefinition.from_op(
                    _op,
                    keys_by_input_name=cd.keys_by_input_name,
                    keys_by_output_name=cd.keys_by_output_name,
                )
                for cd in data
            ]

    my_cacheable_assets_cool = MyCacheableAssets("foo").with_attributes(
        group_names_by_key={AssetKey("foo"): "my_cool_group"}
    )

    my_lame_group_sel = AssetSelection.groups("my_lame_group")
    assert (
        len(
            my_lame_group_sel.resolve(
                my_cacheable_assets_cool.build_definitions(
                    my_cacheable_assets_cool.compute_cacheable_data()
                )
            )
        )
        == 0
    )

    my_cool_group_sel = AssetSelection.groups("my_cool_group")
    assert (
        len(
            my_cool_group_sel.resolve(
                my_cacheable_assets_cool.build_definitions(
                    my_cacheable_assets_cool.compute_cacheable_data()
                )
            )
        )
        == 1
    )


def test_multiple_wrapped_cached_assets():
    """Test that multiple wrappers (with_attributes, with_resources) work properly on cacheable assets."""

    @resource
    def foo_resource():
        return 3

    my_cacheable_assets_with_group_and_asset = [
        x.with_attributes(
            output_asset_key_replacements={
                AssetKey("res_downstream"): AssetKey("res_downstream_too")
            }
        )
        for x in with_resources(
            [
                x.with_attributes(group_names_by_key={AssetKey("res_midstream"): "my_cool_group"})
                for x in define_uncacheable_and_resource_dependent_cacheable_assets()
            ],
            {"foo": foo_resource},
        )
    ]

    with temp_definitions_load_context():

        @repository
        def resource_dependent_repo_with_resources():
            return [
                my_cacheable_assets_with_group_and_asset,
                define_asset_job(
                    "all_asset_job",
                ),
            ]

        repo = resource_dependent_repo_with_resources
        assert isinstance(repo, RepositoryDefinition)
        assert isinstance(repo.get_job("all_asset_job"), JobDefinition)

        my_cool_group_sel = AssetSelection.groups("my_cool_group")
        assert (
            len(
                my_cool_group_sel.resolve(
                    my_cacheable_assets_with_group_and_asset[0].build_definitions(
                        my_cacheable_assets_with_group_and_asset[0].compute_cacheable_data()
                    )
                    + my_cacheable_assets_with_group_and_asset[1:]
                )
            )
            == 1
        )
