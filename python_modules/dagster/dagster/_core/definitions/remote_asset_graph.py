import itertools
import warnings
from collections import defaultdict
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from typing_extensions import Annotated

import dagster._check as check
from dagster._core.definitions.asset_check_spec import AssetCheckKey
from dagster._core.definitions.asset_job import IMPLICIT_ASSET_JOB_NAME
from dagster._core.definitions.asset_key import EntityKey
from dagster._core.definitions.asset_spec import AssetExecutionType
from dagster._core.definitions.auto_materialize_policy import AutoMaterializePolicy
from dagster._core.definitions.backfill_policy import BackfillPolicy
from dagster._core.definitions.base_asset_graph import (
    AssetCheckNode,
    AssetKey,
    BaseAssetGraph,
    BaseAssetNode,
)
from dagster._core.definitions.declarative_automation.automation_condition import (
    AutomationCondition,
)
from dagster._core.definitions.freshness_policy import FreshnessPolicy
from dagster._core.definitions.metadata import ArbitraryMetadataMapping
from dagster._core.definitions.partition import PartitionsDefinition
from dagster._core.definitions.partition_mapping import PartitionMapping
from dagster._core.definitions.selector import RepositorySelector
from dagster._core.definitions.utils import DEFAULT_GROUP_NAME
from dagster._core.remote_representation.external import ExternalRepository
from dagster._record import ImportFrom, record
from dagster._serdes import whitelist_for_serdes

if TYPE_CHECKING:
    from dagster._core.remote_representation.external_data import (
        ExternalAssetCheck,
        ExternalAssetNode,
    )
    from dagster._core.selector.subset_selector import DependencyGraph


@whitelist_for_serdes
@record
class SelectAssetNode:
    selector: RepositorySelector
    asset: Annotated[
        "ExternalAssetNode", ImportFrom("dagster._core.remote_representation.external_data")
    ]


@whitelist_for_serdes
@record
class RemoteAssetNode(BaseAssetNode):
    key: AssetKey
    parent_keys: AbstractSet[AssetKey]
    child_keys: AbstractSet[AssetKey]
    execution_set_entity_keys: AbstractSet[EntityKey]
    select_asset_nodes: Sequence[SelectAssetNode]
    check_keys: AbstractSet[AssetCheckKey]

    @cached_property
    def _external_asset_nodes(self) -> Sequence["ExternalAssetNode"]:
        return [n.asset for n in self.select_asset_nodes]

    ##### COMMON ASSET NODE INTERFACE

    @property
    def description(self) -> Optional[str]:
        return self.priority_node.description

    @property
    def group_name(self) -> str:
        return self.priority_node.group_name or DEFAULT_GROUP_NAME

    @cached_property
    def is_materializable(self) -> bool:
        return any(node.is_materializable for node in self._external_asset_nodes)

    @cached_property
    def is_observable(self) -> bool:
        return any(node.is_observable for node in self._external_asset_nodes)

    @cached_property
    def is_external(self) -> bool:
        return all(node.is_external for node in self._external_asset_nodes)

    @cached_property
    def is_executable(self) -> bool:
        return any(node.is_executable for node in self._external_asset_nodes)

    @property
    def metadata(self) -> ArbitraryMetadataMapping:
        return self.priority_node.metadata

    @property
    def tags(self) -> Mapping[str, str]:
        return self.priority_node.tags or {}

    @property
    def owners(self) -> Sequence[str]:
        return self.priority_node.owners or []

    @property
    def is_partitioned(self) -> bool:
        return self.priority_node.partitions_def_data is not None

    @cached_property
    def partitions_def(self) -> Optional[PartitionsDefinition]:
        external_def = self.priority_node.partitions_def_data
        return external_def.get_partitions_definition() if external_def else None

    @property
    def partition_mappings(self) -> Mapping[AssetKey, PartitionMapping]:
        if self.is_materializable:
            return {
                dep.upstream_asset_key: dep.partition_mapping
                for dep in self._materializable_node.dependencies
                if dep.partition_mapping is not None
            }
        else:
            return {}

    @property
    def freshness_policy(self) -> Optional[FreshnessPolicy]:
        # It is currently not possible to access the freshness policy for an observation definition
        # if a materialization definition also exists. This needs to be fixed.
        return self.priority_node.freshness_policy

    @property
    def auto_materialize_policy(self) -> Optional[AutoMaterializePolicy]:
        return self._materializable_node.auto_materialize_policy if self.is_materializable else None

    @property
    def automation_condition(self) -> Optional[AutomationCondition]:
        if self.is_materializable:
            return self._materializable_node.automation_condition
        elif self.is_observable:
            return self._observable_node.automation_condition
        else:
            return None

    @property
    def auto_observe_interval_minutes(self) -> Optional[float]:
        return self._observable_node.auto_observe_interval_minutes if self.is_observable else None

    @property
    def backfill_policy(self) -> Optional[BackfillPolicy]:
        return self._materializable_node.backfill_policy if self.is_materializable else None

    @property
    def code_version(self) -> Optional[str]:
        # It is currently not possible to access the code version for an observation definition if a
        # materialization definition also exists. This needs to be fixed.
        return self.priority_node.code_version

    @property
    def execution_set_asset_keys(self) -> AbstractSet[AssetKey]:
        return {k for k in self.execution_set_entity_keys if isinstance(k, AssetKey)}

    ##### REMOTE-SPECIFIC INTERFACE

    @property
    def job_names(self) -> Sequence[str]:
        # It is currently not possible to access the job names for an observation definition if a
        # materialization definition also exists. This needs to be fixed.
        return self.priority_node.job_names if self.is_executable else []

    @cached_property
    def _priority_select_node(self) -> SelectAssetNode:
        # Return a materialization node if it exists, otherwise return an observable node if it
        # exists, otherwise return any node. This exists to preserve implicit behavior, where the
        # materialization node was previously preferred over the observable node. This is a
        # temporary measure until we can appropriately scope the accessors that could apply to
        # either a materialization or observation node.

        # This property supports existing behavior but it should be phased out, because it relies on
        # materialization nodes shadowing observation nodes that would otherwise be exposed.
        return next(
            itertools.chain(
                (node for node in self.select_asset_nodes if node.asset.is_materializable),
                (node for node in self.select_asset_nodes if node.asset.is_observable),
                (node for node in self.select_asset_nodes),
            )
        )

    @property
    def priority_repository_selector(self) -> RepositorySelector:
        return self._priority_select_node.selector

    @property
    def repository_selectors(self) -> Sequence[RepositorySelector]:
        return [n.selector for n in self.select_asset_nodes]

    @property
    def priority_node(self) -> "ExternalAssetNode":
        return self._priority_select_node.asset

    ##### HELPERS

    @cached_property
    def _materializable_node(self) -> "ExternalAssetNode":
        try:
            return next(node for node in self._external_asset_nodes if node.is_materializable)
        except StopIteration:
            check.failed("No materializable node found")

    @cached_property
    def _observable_node(self) -> "ExternalAssetNode":
        try:
            return next((node for node in self._external_asset_nodes if node.is_observable))
        except StopIteration:
            check.failed("No observable node found")


class RemoteAssetGraph(BaseAssetGraph[RemoteAssetNode]):
    def __init__(
        self,
        asset_nodes_by_key: Mapping[AssetKey, RemoteAssetNode],
        asset_checks_by_key: Mapping[AssetCheckKey, "ExternalAssetCheck"],
        asset_check_execution_sets_by_key: Mapping[AssetCheckKey, AbstractSet[EntityKey]],
        repository_selectors_by_asset_check_key: Mapping[AssetCheckKey, RepositorySelector],
    ):
        self._asset_nodes_by_key = asset_nodes_by_key
        self._asset_checks_by_key = asset_checks_by_key
        self._asset_check_nodes_by_key = {
            k: AssetCheckNode(k, v.blocking, v.automation_condition)
            for k, v in asset_checks_by_key.items()
        }
        self._asset_check_execution_sets_by_key = asset_check_execution_sets_by_key
        self._repository_handles_by_asset_check_key = repository_selectors_by_asset_check_key

    @classmethod
    def from_repository_selectors_and_external_asset_nodes(
        cls,
        repo_handle_assets: Sequence[Tuple[RepositorySelector, "ExternalAssetNode"]],
        repo_handle_asset_checks: Sequence[Tuple[RepositorySelector, "ExternalAssetCheck"]],
    ) -> "RemoteAssetGraph":
        _warn_on_duplicate_nodes(repo_handle_assets)

        # Build an index of execution sets by key. An execution set is a set of assets and checks
        # that must be executed together. ExternalAssetNodes and ExternalAssetChecks already have an
        # optional execution_set_identifier set. A null execution_set_identifier indicates that the
        # node or check can be executed independently.
        assets = [asset for _, asset in repo_handle_assets]
        asset_checks = [asset_check for _, asset_check in repo_handle_asset_checks]
        execution_sets_by_key = _build_execution_set_index(assets, asset_checks)

        # Index all (RepositorySelector, ExternalAssetNode) pairs by their asset key, then use this to
        # build the set of RemoteAssetNodes (indexed by key). Each RemoteAssetNode wraps the set of
        # pairs for an asset key.
        repo_node_pairs_by_key: Dict[
            AssetKey, List[Tuple[RepositorySelector, "ExternalAssetNode"]]
        ] = defaultdict(list)

        # Build the dependency graph of asset keys.
        all_keys = {asset.asset_key for asset in assets}
        upstream: Dict[AssetKey, Set[AssetKey]] = {key: set() for key in all_keys}
        downstream: Dict[AssetKey, Set[AssetKey]] = {key: set() for key in all_keys}

        for selector, node in repo_handle_assets:
            repo_node_pairs_by_key[node.asset_key].append((selector, node))
            for dep in node.dependencies:
                upstream[node.asset_key].add(dep.upstream_asset_key)
                downstream[dep.upstream_asset_key].add(node.asset_key)

        dep_graph: DependencyGraph[AssetKey] = {"upstream": upstream, "downstream": downstream}

        # Build the set of ExternalAssetChecks, indexed by key. Also the index of execution units for
        # each asset check key.
        check_keys_by_asset_key: Dict[AssetKey, Set[AssetCheckKey]] = defaultdict(set)
        asset_checks_by_key: Dict[AssetCheckKey, "ExternalAssetCheck"] = {}
        repository_handles_by_asset_check_key: Dict[AssetCheckKey, RepositorySelector] = {}
        for repo_handle, asset_check in repo_handle_asset_checks:
            asset_checks_by_key[asset_check.key] = asset_check
            check_keys_by_asset_key[asset_check.asset_key].add(asset_check.key)
            repository_handles_by_asset_check_key[asset_check.key] = selector

        asset_check_execution_sets_by_key = {
            k: v for k, v in execution_sets_by_key.items() if isinstance(k, AssetCheckKey)
        }
        # Build the set of RemoteAssetNodes in topological order so that each node can hold
        # references to its parents.
        asset_nodes_by_key = {
            key: RemoteAssetNode(
                key=key,
                parent_keys=dep_graph["upstream"][key],
                child_keys=dep_graph["downstream"][key],
                execution_set_entity_keys=execution_sets_by_key[key],
                select_asset_nodes=[
                    SelectAssetNode(selector=selector, asset=asset)
                    for selector, asset in repo_node_pairs_by_key[key]
                ],
                check_keys=check_keys_by_asset_key[key],
            )
            for key, repo_node_pairs in repo_node_pairs_by_key.items()
        }

        return cls(
            asset_nodes_by_key,
            asset_checks_by_key,
            asset_check_execution_sets_by_key,
            repository_handles_by_asset_check_key,
        )

    ##### COMMON ASSET GRAPH INTERFACE

    def get_execution_set_asset_and_check_keys(
        self, entity_key: EntityKey
    ) -> AbstractSet[EntityKey]:
        if isinstance(entity_key, AssetKey):
            return self.get(entity_key).execution_set_entity_keys
        else:  # AssetCheckKey
            return self._asset_check_execution_sets_by_key[entity_key]

    ##### REMOTE-SPECIFIC METHODS

    @property
    def external_asset_nodes_by_key(self) -> Mapping[AssetKey, "ExternalAssetNode"]:
        # This exists to support existing callsites but it should be removed ASAP, since it exposes
        # `ExternalAssetNode` instances directly. All sites using this should use RemoteAssetNode
        # instead.
        return {k: node.priority_node for k, node in self._asset_nodes_by_key.items()}

    @property
    def asset_checks(self) -> Sequence["ExternalAssetCheck"]:
        return list(self._asset_checks_by_key.values())

    @cached_property
    def asset_check_keys(self) -> AbstractSet[AssetCheckKey]:
        return set(self._asset_checks_by_key.keys())

    def asset_keys_for_job(self, job_name: str) -> AbstractSet[AssetKey]:
        return {node.key for node in self.asset_nodes if job_name in node.job_names}

    @cached_property
    def all_job_names(self) -> AbstractSet[str]:
        return {job_name for node in self.asset_nodes for job_name in node.job_names}

    @cached_property
    def repository_selectors_by_key(self) -> Mapping[EntityKey, RepositorySelector]:
        return {
            **{
                k: node.priority_repository_selector for k, node in self._asset_nodes_by_key.items()
            },
            **self._repository_handles_by_asset_check_key,
        }

    def get_repository_selector(self, key: EntityKey) -> RepositorySelector:
        return self.repository_selectors_by_key[key]

    def get_materialization_job_names(self, asset_key: AssetKey) -> Sequence[str]:
        """Returns the names of jobs that materialize this asset."""
        # This is a poorly named method because it will expose observation job names for assets with
        # a defined observation but no materialization.
        return self.get(asset_key).job_names

    def get_materialization_asset_keys_for_job(self, job_name: str) -> Sequence[AssetKey]:
        """Returns asset keys that are targeted for materialization in the given job."""
        return [
            k
            for k in self.materializable_asset_keys
            if job_name in self.get_materialization_job_names(k)
        ]

    def get_implicit_job_name_for_assets(
        self,
        asset_keys: Iterable[AssetKey],
        external_repo: Optional[ExternalRepository],
    ) -> Optional[str]:
        """Returns the name of the asset base job that contains all the given assets, or None if there is no such
        job.

        Note: all asset_keys should be in the same repository.
        """
        return IMPLICIT_ASSET_JOB_NAME

    def split_entity_keys_by_repository(
        self, keys: AbstractSet[EntityKey]
    ) -> Sequence[AbstractSet[EntityKey]]:
        keys_by_repo = defaultdict(set)
        for key in keys:
            repo_handle = self.get_repository_selector(key)
            keys_by_repo[(repo_handle.location_name, repo_handle.repository_name)].add(key)
        return list(keys_by_repo.values())


def _warn_on_duplicate_nodes(
    repo_selector_external_asset_nodes: Sequence[Tuple[RepositorySelector, "ExternalAssetNode"]],
) -> None:
    # Split the nodes into materializable, observable, and unexecutable nodes. Observable and
    # unexecutable `ExternalAssetNode` represent both source and external assets-- the
    # "External" in "ExternalAssetNode" is unrelated to the "external" in "external asset", this
    # is just an unfortunate naming collision. `ExternalAssetNode` will be renamed eventually.
    materializable_node_pairs: List[Tuple[RepositorySelector, "ExternalAssetNode"]] = []
    observable_node_pairs: List[Tuple[RepositorySelector, "ExternalAssetNode"]] = []
    unexecutable_node_pairs: List[Tuple[RepositorySelector, "ExternalAssetNode"]] = []
    for repo_selector, node in repo_selector_external_asset_nodes:
        if node.is_source and node.is_observable:
            observable_node_pairs.append((repo_selector, node))
        elif node.is_source:
            unexecutable_node_pairs.append((repo_selector, node))
        else:
            materializable_node_pairs.append((repo_selector, node))

    # It is possible for multiple nodes to exist that share the same key. This is invalid if
    # more than one node is materializable or if more than one node is observable. It is valid
    # if there is at most one materializable node and at most one observable node, with all
    # other nodes unexecutable.
    _warn_on_duplicates_within_subset(materializable_node_pairs, AssetExecutionType.MATERIALIZATION)
    _warn_on_duplicates_within_subset(observable_node_pairs, AssetExecutionType.OBSERVATION)


def _warn_on_duplicates_within_subset(
    node_pairs: Sequence[Tuple[RepositorySelector, "ExternalAssetNode"]],
    execution_type: AssetExecutionType,
) -> None:
    repo_handles_by_asset_key: DefaultDict[AssetKey, List[RepositorySelector]] = defaultdict(list)
    for repo_handle, node in node_pairs:
        repo_handles_by_asset_key[node.asset_key].append(repo_handle)

    duplicates = {k: v for k, v in repo_handles_by_asset_key.items() if len(v) > 1}
    duplicate_lines = []
    for asset_key, selectors in duplicates.items():
        locations = [selector.location_name for selector in selectors]
        duplicate_lines.append(f"  {asset_key.to_string()}: {locations}")
    duplicate_str = "\n".join(duplicate_lines)
    if duplicates:
        warnings.warn(
            f"Found {execution_type.value} nodes for some asset keys in multiple code locations."
            f" Only one {execution_type.value} node is allowed per asset key. Duplicates:\n {duplicate_str}"
        )


def _build_execution_set_index(
    external_asset_nodes: Iterable["ExternalAssetNode"],
    external_asset_checks: Iterable["ExternalAssetCheck"],
) -> Mapping[EntityKey, AbstractSet[EntityKey]]:
    from dagster._core.remote_representation.external_data import ExternalAssetNode

    all_items = [*external_asset_nodes, *external_asset_checks]

    execution_sets_by_id: Dict[str, Set[EntityKey]] = defaultdict(set)
    for item in all_items:
        id = item.execution_set_identifier
        key = item.asset_key if isinstance(item, ExternalAssetNode) else item.key
        if id is not None:
            execution_sets_by_id[id].add(key)

    execution_sets_by_key: Dict[EntityKey, Set[EntityKey]] = {}
    for item in all_items:
        id = item.execution_set_identifier
        key = item.asset_key if isinstance(item, ExternalAssetNode) else item.key
        execution_sets_by_key[key] = execution_sets_by_id[id] if id is not None else {key}

    return execution_sets_by_key
