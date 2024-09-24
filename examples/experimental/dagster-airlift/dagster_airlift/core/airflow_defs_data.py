from typing import AbstractSet, List, Optional

from dagster import AssetKey, AssetsDefinition, AssetSpec, external_asset_from_spec
from dagster._core.definitions.asset_key import AssetCheckKey
from dagster._record import record
from dagster._serdes.serdes import whitelist_for_serdes

from dagster_airlift.core.serialization.serialized_data import SerializedAirflowDefinitionsData


@whitelist_for_serdes
@record
class AirflowDefinitionsData:
    instance_name: str
    serialized_data: SerializedAirflowDefinitionsData

    def map_airflow_data_to_spec(self, spec: AssetSpec) -> AssetSpec:
        """If there is airflow data applicable to the asset key, transform the spec and apply the data."""
        existing_key_data = self.serialized_data.existing_asset_data[spec.key].existing_key_data
        return spec if not existing_key_data else existing_key_data.apply_to_spec(spec)

    def construct_dag_assets_defs(self) -> List[AssetsDefinition]:
        return [
            external_asset_from_spec(dag_data.spec_data.to_asset_spec())
            for dag_data in self.serialized_data.dag_datas.values()
        ]

    def get_check_keys_for_asset_key(self, asset_key: AssetKey) -> AbstractSet[AssetCheckKey]:
        return self.serialized_data.existing_asset_data[asset_key].asset_graph_data.check_keys

    @property
    def all_dag_ids(self) -> AbstractSet[str]:
        return set(self.serialized_data.dag_datas.keys())

    def asset_key_for_dag(self, dag_id: str) -> AssetKey:
        return self.serialized_data.dag_datas[dag_id].spec_data.asset_key

    def task_ids_in_dag(self, dag_id: str) -> AbstractSet[str]:
        return set(self.serialized_data.dag_datas[dag_id].task_handle_data.keys())

    def migration_state_for_task(self, dag_id: str, task_id: str) -> Optional[bool]:
        return self.serialized_data.dag_datas[dag_id].task_handle_data[task_id].migration_state

    def asset_keys_in_task(self, dag_id: str, task_id: str) -> AbstractSet[AssetKey]:
        return self.serialized_data.dag_datas[dag_id].task_handle_data[task_id].asset_keys_in_task

    def check_keys_for_asset_key(self, asset_key: AssetKey) -> AbstractSet[AssetCheckKey]:
        return self.serialized_data.existing_asset_data[asset_key].asset_graph_data.check_keys

    def topo_order_index(self, asset_key: AssetKey) -> int:
        return self.serialized_data.asset_key_topological_ordering.index(asset_key)
