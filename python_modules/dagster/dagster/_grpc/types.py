import base64
import zlib
from enum import Enum
from typing import Any, FrozenSet, Mapping, NamedTuple, Optional, Sequence, Type, TypeVar

import dagster._check as check
from dagster._config.structured_config.readiness_check import ReadinessCheckResult
from dagster._core.code_pointer import CodePointer
from dagster._core.definitions.events import AssetKey
from dagster._core.execution.plan.state import KnownExecutionState
from dagster._core.execution.retries import RetryMode
from dagster._core.host_representation.origin import (
    CodeLocationOrigin,
    ExternalPipelineOrigin,
    ExternalRepositoryOrigin,
)
from dagster._core.instance.ref import InstanceRef
from dagster._core.origin import PipelinePythonOrigin, get_python_environment_entry_point
from dagster._serdes import serialize_value, whitelist_for_serdes
from dagster._utils.error import SerializableErrorInfo


@whitelist_for_serdes
class ExecutionPlanSnapshotArgs(
    NamedTuple(
        "_ExecutionPlanSnapshotArgs",
        [
            ("pipeline_origin", ExternalPipelineOrigin),
            ("solid_selection", Sequence[str]),
            ("run_config", Mapping[str, object]),
            ("mode", str),
            ("step_keys_to_execute", Optional[Sequence[str]]),
            ("pipeline_snapshot_id", str),
            ("known_state", Optional[KnownExecutionState]),
            ("instance_ref", Optional[InstanceRef]),
            ("asset_selection", Optional[FrozenSet[AssetKey]]),
        ],
    )
):
    def __new__(
        cls,
        pipeline_origin: ExternalPipelineOrigin,
        solid_selection: Sequence[str],
        run_config: Mapping[str, object],
        mode: str,
        step_keys_to_execute: Optional[Sequence[str]],
        pipeline_snapshot_id: str,
        known_state: Optional[KnownExecutionState] = None,
        instance_ref: Optional[InstanceRef] = None,
        asset_selection: Optional[FrozenSet[AssetKey]] = None,
    ):
        return super(ExecutionPlanSnapshotArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin, "pipeline_origin", ExternalPipelineOrigin
            ),
            solid_selection=check.opt_sequence_param(
                solid_selection, "solid_selection", of_type=str
            ),
            run_config=check.mapping_param(run_config, "run_config", key_type=str),
            mode=check.str_param(mode, "mode"),
            step_keys_to_execute=check.opt_nullable_sequence_param(
                step_keys_to_execute, "step_keys_to_execute", of_type=str
            ),
            pipeline_snapshot_id=check.str_param(pipeline_snapshot_id, "pipeline_snapshot_id"),
            known_state=check.opt_inst_param(known_state, "known_state", KnownExecutionState),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            asset_selection=check.opt_nullable_set_param(
                asset_selection, "asset_selection", of_type=AssetKey
            ),
        )


def _get_entry_point(origin: PipelinePythonOrigin):
    return (
        origin.repository_origin.entry_point
        if origin.repository_origin.entry_point
        else get_python_environment_entry_point(origin.executable_path)
    )


@whitelist_for_serdes
class ExecuteRunArgs(
    NamedTuple(
        "_ExecuteRunArgs",
        [
            # Deprecated, only needed for back-compat since it can be pulled from the PipelineRun
            ("pipeline_origin", PipelinePythonOrigin),
            ("pipeline_run_id", str),
            ("instance_ref", Optional[InstanceRef]),
            ("set_exit_code_on_failure", Optional[bool]),
        ],
    )
):
    def __new__(
        cls,
        pipeline_origin: PipelinePythonOrigin,
        pipeline_run_id: str,
        instance_ref: Optional[InstanceRef],
        set_exit_code_on_failure: Optional[bool] = None,
    ):
        return super(ExecuteRunArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin,
                "pipeline_origin",
                PipelinePythonOrigin,
            ),
            pipeline_run_id=check.str_param(pipeline_run_id, "pipeline_run_id"),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            set_exit_code_on_failure=(
                True
                if check.opt_bool_param(set_exit_code_on_failure, "set_exit_code_on_failure")
                is True
                else None
            ),  # for back-compat
        )

    def get_command_args(self) -> Sequence[str]:
        return _get_entry_point(self.pipeline_origin) + [
            "api",
            "execute_run",
            serialize_value(self),
        ]


@whitelist_for_serdes
class ResumeRunArgs(
    NamedTuple(
        "_ResumeRunArgs",
        [
            # Deprecated, only needed for back-compat since it can be pulled from the PipelineRun
            ("pipeline_origin", PipelinePythonOrigin),
            ("pipeline_run_id", str),
            ("instance_ref", Optional[InstanceRef]),
            ("set_exit_code_on_failure", Optional[bool]),
        ],
    )
):
    def __new__(
        cls,
        pipeline_origin: PipelinePythonOrigin,
        pipeline_run_id: str,
        instance_ref: Optional[InstanceRef],
        set_exit_code_on_failure: Optional[bool] = None,
    ):
        return super(ResumeRunArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin,
                "pipeline_origin",
                PipelinePythonOrigin,
            ),
            pipeline_run_id=check.str_param(pipeline_run_id, "pipeline_run_id"),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            set_exit_code_on_failure=(
                True
                if check.opt_bool_param(set_exit_code_on_failure, "set_exit_code_on_failure")
                is True
                else None
            ),  # for back-compat
        )

    def get_command_args(self) -> Sequence[str]:
        return _get_entry_point(self.pipeline_origin) + [
            "api",
            "resume_run",
            serialize_value(self),
        ]


@whitelist_for_serdes
class ExecuteExternalPipelineArgs(
    NamedTuple(
        "_ExecuteExternalPipelineArgs",
        [
            ("pipeline_origin", ExternalPipelineOrigin),
            ("pipeline_run_id", str),
            ("instance_ref", Optional[InstanceRef]),
        ],
    )
):
    def __new__(
        cls,
        pipeline_origin: ExternalPipelineOrigin,
        pipeline_run_id: str,
        instance_ref: Optional[InstanceRef],
    ):
        return super(ExecuteExternalPipelineArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin,
                "pipeline_origin",
                ExternalPipelineOrigin,
            ),
            pipeline_run_id=check.str_param(pipeline_run_id, "pipeline_run_id"),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
        )


@whitelist_for_serdes
class ExecuteStepArgs(
    NamedTuple(
        "_ExecuteStepArgs",
        [
            # Deprecated, only needed for back-compat since it can be pulled from the PipelineRun
            ("pipeline_origin", PipelinePythonOrigin),
            ("pipeline_run_id", str),
            ("step_keys_to_execute", Optional[Sequence[str]]),
            ("instance_ref", Optional[InstanceRef]),
            ("retry_mode", Optional[RetryMode]),
            ("known_state", Optional[KnownExecutionState]),
            ("should_verify_step", Optional[bool]),
        ],
    )
):
    def __new__(
        cls,
        pipeline_origin: PipelinePythonOrigin,
        pipeline_run_id: str,
        step_keys_to_execute: Optional[Sequence[str]],
        instance_ref: Optional[InstanceRef] = None,
        retry_mode: Optional[RetryMode] = None,
        known_state: Optional[KnownExecutionState] = None,
        should_verify_step: Optional[bool] = None,
    ):
        return super(ExecuteStepArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin, "pipeline_origin", PipelinePythonOrigin
            ),
            pipeline_run_id=check.str_param(pipeline_run_id, "pipeline_run_id"),
            step_keys_to_execute=check.opt_nullable_sequence_param(
                step_keys_to_execute, "step_keys_to_execute", of_type=str
            ),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            retry_mode=check.opt_inst_param(retry_mode, "retry_mode", RetryMode),
            known_state=check.opt_inst_param(known_state, "known_state", KnownExecutionState),
            should_verify_step=check.opt_bool_param(
                should_verify_step, "should_verify_step", False
            ),
        )

    def _get_compressed_args(self) -> str:
        # Compress, then base64 encode so we can pass it around as a str
        return base64.b64encode(zlib.compress(serialize_value(self).encode())).decode()

    def get_command_args(self, skip_serialized_namedtuple: bool = False) -> Sequence[str]:
        """Get the command args to run this step. If skip_serialized_namedtuple is True, then get_command_env should
        be used to pass the args to Click using an env var.
        """
        return (
            _get_entry_point(self.pipeline_origin)
            + ["api", "execute_step"]
            + (
                ["--compressed-input-json", self._get_compressed_args()]
                if not skip_serialized_namedtuple
                else []
            )
        )

    def get_command_env(self) -> Sequence[Mapping[str, str]]:
        """Get the env vars for overriding the Click args of this step. Used in conjuction with
        get_command_args(skip_serialized_namedtuple=True).
        """
        return [
            {"name": "DAGSTER_COMPRESSED_EXECUTE_STEP_ARGS", "value": self._get_compressed_args()},
        ]


@whitelist_for_serdes
class LoadableRepositorySymbol(
    NamedTuple("_LoadableRepositorySymbol", [("repository_name", str), ("attribute", str)])
):
    def __new__(cls, repository_name: str, attribute: str):
        return super(LoadableRepositorySymbol, cls).__new__(
            cls,
            repository_name=check.str_param(repository_name, "repository_name"),
            attribute=check.str_param(attribute, "attribute"),
        )


@whitelist_for_serdes
class ListRepositoriesResponse(
    NamedTuple(
        "_ListRepositoriesResponse",
        [
            ("repository_symbols", Sequence[LoadableRepositorySymbol]),
            ("executable_path", Optional[str]),
            ("repository_code_pointer_dict", Mapping[str, CodePointer]),
            ("entry_point", Optional[Sequence[str]]),
            ("container_image", Optional[str]),
            ("container_context", Optional[Mapping[str, Any]]),
            ("dagster_library_versions", Optional[Mapping[str, str]]),
        ],
    )
):
    def __new__(
        cls,
        repository_symbols: Sequence[LoadableRepositorySymbol],
        executable_path: Optional[str] = None,
        repository_code_pointer_dict: Optional[Mapping[str, CodePointer]] = None,
        entry_point: Optional[Sequence[str]] = None,
        container_image: Optional[str] = None,
        container_context: Optional[Mapping] = None,
        dagster_library_versions: Optional[Mapping[str, str]] = None,
    ):
        return super(ListRepositoriesResponse, cls).__new__(
            cls,
            repository_symbols=check.sequence_param(
                repository_symbols, "repository_symbols", of_type=LoadableRepositorySymbol
            ),
            executable_path=check.opt_str_param(executable_path, "executable_path"),
            repository_code_pointer_dict=check.opt_mapping_param(
                repository_code_pointer_dict,
                "repository_code_pointer_dict",
                key_type=str,
                value_type=CodePointer,
            ),
            entry_point=(
                check.sequence_param(entry_point, "entry_point", of_type=str)
                if entry_point is not None
                else None
            ),
            container_image=check.opt_str_param(container_image, "container_image"),
            container_context=(
                check.dict_param(container_context, "container_context")
                if container_context is not None
                else None
            ),
            dagster_library_versions=check.opt_nullable_mapping_param(
                dagster_library_versions, "dagster_library_versions"
            ),
        )


@whitelist_for_serdes
class ListRepositoriesInput(
    NamedTuple(
        "_ListRepositoriesInput",
        [
            ("module_name", Optional[str]),
            ("python_file", Optional[str]),
            ("working_directory", Optional[str]),
            ("attribute", Optional[str]),
        ],
    )
):
    def __new__(
        cls,
        module_name: Optional[str],
        python_file: Optional[str],
        working_directory: Optional[str],
        attribute: Optional[str],
    ):
        check.invariant(not (module_name and python_file), "Must set only one")
        check.invariant(module_name or python_file, "Must set at least one")
        return super(ListRepositoriesInput, cls).__new__(
            cls,
            module_name=check.opt_str_param(module_name, "module_name"),
            python_file=check.opt_str_param(python_file, "python_file"),
            working_directory=check.opt_str_param(working_directory, "working_directory"),
            attribute=check.opt_str_param(attribute, "attribute"),
        )


@whitelist_for_serdes
class PartitionArgs(
    NamedTuple(
        "_PartitionArgs",
        [
            ("repository_origin", ExternalRepositoryOrigin),
            ("partition_set_name", str),
            ("partition_name", str),
            ("instance_ref", Optional[InstanceRef]),
        ],
    )
):
    def __new__(
        cls,
        repository_origin: ExternalRepositoryOrigin,
        partition_set_name: str,
        partition_name: str,
        instance_ref: Optional[InstanceRef] = None,
    ):
        return super(PartitionArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin,
                "repository_origin",
                ExternalRepositoryOrigin,
            ),
            partition_set_name=check.str_param(partition_set_name, "partition_set_name"),
            partition_name=check.str_param(partition_name, "partition_name"),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
        )


@whitelist_for_serdes
class PartitionNamesArgs(
    NamedTuple(
        "_PartitionNamesArgs",
        [("repository_origin", ExternalRepositoryOrigin), ("partition_set_name", str)],
    )
):
    def __new__(cls, repository_origin: ExternalRepositoryOrigin, partition_set_name: str):
        return super(PartitionNamesArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin, "repository_origin", ExternalRepositoryOrigin
            ),
            partition_set_name=check.str_param(partition_set_name, "partition_set_name"),
        )


@whitelist_for_serdes
class PartitionSetExecutionParamArgs(
    NamedTuple(
        "_PartitionSetExecutionParamArgs",
        [
            ("repository_origin", ExternalRepositoryOrigin),
            ("partition_set_name", str),
            ("partition_names", Sequence[str]),
            ("instance_ref", Optional[InstanceRef]),
        ],
    )
):
    def __new__(
        cls,
        repository_origin: ExternalRepositoryOrigin,
        partition_set_name: str,
        partition_names: Sequence[str],
        instance_ref: Optional[InstanceRef] = None,
    ):
        return super(PartitionSetExecutionParamArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin, "repository_origin", ExternalRepositoryOrigin
            ),
            partition_set_name=check.str_param(partition_set_name, "partition_set_name"),
            partition_names=check.sequence_param(partition_names, "partition_names", of_type=str),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
        )


@whitelist_for_serdes
class PipelineSubsetSnapshotArgs(
    NamedTuple(
        "_PipelineSubsetSnapshotArgs",
        [
            ("pipeline_origin", ExternalPipelineOrigin),
            ("solid_selection", Optional[Sequence[str]]),
            ("asset_selection", Optional[Sequence[AssetKey]]),
        ],
    )
):
    def __new__(
        cls,
        pipeline_origin: ExternalPipelineOrigin,
        solid_selection: Sequence[str],
        asset_selection: Optional[Sequence[AssetKey]] = None,
    ):
        return super(PipelineSubsetSnapshotArgs, cls).__new__(
            cls,
            pipeline_origin=check.inst_param(
                pipeline_origin, "pipeline_origin", ExternalPipelineOrigin
            ),
            solid_selection=check.sequence_param(solid_selection, "solid_selection", of_type=str)
            if solid_selection
            else None,
            asset_selection=check.opt_sequence_param(
                asset_selection, "asset_selection", of_type=AssetKey
            ),
        )


# Different storage field name for backcompat
@whitelist_for_serdes(storage_field_names={"code_location_origin": "repository_location_origin"})
class NotebookPathArgs(
    NamedTuple(
        "_NotebookPathArgs",
        [("code_location_origin", CodeLocationOrigin), ("notebook_path", str)],
    )
):
    def __new__(cls, code_location_origin: CodeLocationOrigin, notebook_path: str):
        return super(NotebookPathArgs, cls).__new__(
            cls,
            code_location_origin=check.inst_param(
                code_location_origin, "code_location_origin", CodeLocationOrigin
            ),
            notebook_path=check.str_param(notebook_path, "notebook_path"),
        )


@whitelist_for_serdes
class ExternalScheduleExecutionArgs(
    NamedTuple(
        "_ExternalScheduleExecutionArgs",
        [
            ("repository_origin", ExternalRepositoryOrigin),
            ("instance_ref", Optional[InstanceRef]),
            ("schedule_name", str),
            ("scheduled_execution_timestamp", Optional[float]),
            ("scheduled_execution_timezone", Optional[str]),
        ],
    )
):
    def __new__(
        cls,
        repository_origin: ExternalRepositoryOrigin,
        instance_ref: Optional[InstanceRef],
        schedule_name: str,
        scheduled_execution_timestamp: Optional[float] = None,
        scheduled_execution_timezone: Optional[str] = None,
    ):
        return super(ExternalScheduleExecutionArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin, "repository_origin", ExternalRepositoryOrigin
            ),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            schedule_name=check.str_param(schedule_name, "schedule_name"),
            scheduled_execution_timestamp=check.opt_float_param(
                scheduled_execution_timestamp, "scheduled_execution_timestamp"
            ),
            scheduled_execution_timezone=check.opt_str_param(
                scheduled_execution_timezone,
                "scheduled_execution_timezone",
            ),
        )


@whitelist_for_serdes
class SensorExecutionArgs(
    NamedTuple(
        "_SensorExecutionArgs",
        [
            ("repository_origin", ExternalRepositoryOrigin),
            ("instance_ref", Optional[InstanceRef]),
            ("sensor_name", str),
            ("last_completion_time", Optional[float]),
            ("last_run_key", Optional[str]),
            ("cursor", Optional[str]),
        ],
    )
):
    def __new__(
        cls,
        repository_origin: ExternalRepositoryOrigin,
        instance_ref: Optional[InstanceRef],
        sensor_name: str,
        last_completion_time: Optional[float],
        last_run_key: Optional[str],
        cursor: Optional[str],
    ):
        return super(SensorExecutionArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin, "repository_origin", ExternalRepositoryOrigin
            ),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            sensor_name=check.str_param(sensor_name, "sensor_name"),
            last_completion_time=check.opt_float_param(
                last_completion_time, "last_completion_time"
            ),
            last_run_key=check.opt_str_param(last_run_key, "last_run_key"),
            cursor=check.opt_str_param(cursor, "cursor"),
        )


@whitelist_for_serdes
class ExternalJobArgs(
    NamedTuple(
        "_ExternalJobArgs",
        [
            ("repository_origin", ExternalRepositoryOrigin),
            ("instance_ref", InstanceRef),
            ("name", str),
        ],
    )
):
    def __new__(
        cls, repository_origin: ExternalRepositoryOrigin, instance_ref: InstanceRef, name: str
    ):
        return super(ExternalJobArgs, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin, "repository_origin", ExternalRepositoryOrigin
            ),
            instance_ref=check.inst_param(instance_ref, "instance_ref", InstanceRef),
            name=check.str_param(name, "name"),
        )


@whitelist_for_serdes
class ShutdownServerResult(
    NamedTuple(
        "_ShutdownServerResult",
        [("success", bool), ("serializable_error_info", Optional[SerializableErrorInfo])],
    )
):
    def __new__(cls, success: bool, serializable_error_info: Optional[SerializableErrorInfo]):
        return super(ShutdownServerResult, cls).__new__(
            cls,
            success=check.bool_param(success, "success"),
            serializable_error_info=check.opt_inst_param(
                serializable_error_info, "serializable_error_info", SerializableErrorInfo
            ),
        )


@whitelist_for_serdes
class CancelExecutionRequest(NamedTuple("_CancelExecutionRequest", [("run_id", str)])):
    def __new__(cls, run_id: str):
        return super(CancelExecutionRequest, cls).__new__(
            cls,
            run_id=check.str_param(run_id, "run_id"),
        )


@whitelist_for_serdes
class CancelExecutionResult(
    NamedTuple(
        "_CancelExecutionResult",
        [
            ("success", bool),
            ("message", Optional[str]),
            ("serializable_error_info", Optional[SerializableErrorInfo]),
        ],
    )
):
    def __new__(
        cls,
        success: bool,
        message: Optional[str],
        serializable_error_info: Optional[SerializableErrorInfo],
    ):
        return super(CancelExecutionResult, cls).__new__(
            cls,
            success=check.bool_param(success, "success"),
            message=check.opt_str_param(message, "message"),
            serializable_error_info=check.opt_inst_param(
                serializable_error_info, "serializable_error_info", SerializableErrorInfo
            ),
        )


@whitelist_for_serdes
class CanCancelExecutionRequest(NamedTuple("_CanCancelExecutionRequest", [("run_id", str)])):
    def __new__(cls, run_id: str):
        return super(CanCancelExecutionRequest, cls).__new__(
            cls,
            run_id=check.str_param(run_id, "run_id"),
        )


@whitelist_for_serdes
class CanCancelExecutionResult(NamedTuple("_CancelExecutionResult", [("can_cancel", bool)])):
    def __new__(cls, can_cancel: bool):
        return super(CanCancelExecutionResult, cls).__new__(
            cls,
            can_cancel=check.bool_param(can_cancel, "can_cancel"),
        )


@whitelist_for_serdes
class StartRunResult(
    NamedTuple(
        "_StartRunResult",
        [
            ("success", bool),
            ("message", Optional[str]),
            ("serializable_error_info", Optional[SerializableErrorInfo]),
        ],
    )
):
    def __new__(
        cls,
        success: bool,
        message: Optional[str],
        serializable_error_info: Optional[SerializableErrorInfo],
    ):
        return super(StartRunResult, cls).__new__(
            cls,
            success=check.bool_param(success, "success"),
            message=check.opt_str_param(message, "message"),
            serializable_error_info=check.opt_inst_param(
                serializable_error_info, "serializable_error_info", SerializableErrorInfo
            ),
        )


@whitelist_for_serdes
class GetCurrentImageResult(
    NamedTuple(
        "_GetCurrentImageResult",
        [
            ("current_image", Optional[str]),
            ("serializable_error_info", Optional[SerializableErrorInfo]),
        ],
    )
):
    def __new__(
        cls, current_image: Optional[str], serializable_error_info: Optional[SerializableErrorInfo]
    ):
        return super(GetCurrentImageResult, cls).__new__(
            cls,
            current_image=check.opt_str_param(current_image, "current_image"),
            serializable_error_info=check.opt_inst_param(
                serializable_error_info, "serializable_error_info", SerializableErrorInfo
            ),
        )


@whitelist_for_serdes
class GetCurrentRunsResult(
    NamedTuple(
        "_GetCurrentRunsResult",
        [
            ("current_runs", Sequence[str]),
            ("serializable_error_info", Optional[SerializableErrorInfo]),
        ],
    )
):
    def __new__(
        cls,
        current_runs: Sequence[str],
        serializable_error_info: Optional[SerializableErrorInfo],
    ):
        return super(GetCurrentRunsResult, cls).__new__(
            cls,
            current_runs=check.list_param(current_runs, "current_runs", of_type=str),
            serializable_error_info=check.opt_inst_param(
                serializable_error_info, "serializable_error_info", SerializableErrorInfo
            ),
        )


@whitelist_for_serdes
class ResourceReadinessCheckRequest(
    NamedTuple(
        "_ResourceReadinessCheckRequest",
        [
            ("repository_origin", ExternalRepositoryOrigin),
            ("instance_ref", Optional[InstanceRef]),
            ("resource_name", str),
        ],
    )
):
    def __new__(
        cls,
        repository_origin: ExternalRepositoryOrigin,
        instance_ref: Optional[InstanceRef],
        resource_name: str,
    ):
        return super(ResourceReadinessCheckRequest, cls).__new__(
            cls,
            repository_origin=check.inst_param(
                repository_origin, "repository_origin", ExternalRepositoryOrigin
            ),
            instance_ref=check.opt_inst_param(instance_ref, "instance_ref", InstanceRef),
            resource_name=check.str_param(resource_name, "resource_name"),
        )


@whitelist_for_serdes
class ResourceReadinessCheckResult(
    NamedTuple(
        "_ResourceReadinessCheckResult",
        [
            ("response", ReadinessCheckResult),
            ("serializable_error_info", Optional[SerializableErrorInfo]),
        ],
    )
):
    def __new__(
        cls,
        response: ReadinessCheckResult,
        serializable_error_info: Optional[SerializableErrorInfo],
    ):
        return super(ResourceReadinessCheckResult, cls).__new__(
            cls,
            response=check.inst_param(response, "response", ReadinessCheckResult),
            serializable_error_info=check.opt_inst_param(
                serializable_error_info, "serializable_error_info", SerializableErrorInfo
            ),
        )


@whitelist_for_serdes
class UserCodeExecutionType(Enum):
    """Represents the type of user code execution request.

    This is used to determine and verify specific request and result types when unpacking the generic
    UserCodeExecutionRequest and UserCodeExecutionResult types.
    """

    RESOURCE_READINESS_CHECK = "resource_readiness_check"


T = TypeVar("T")

EXECUTION_TYPE_TO_DATA_MAP = {
    UserCodeExecutionType.RESOURCE_READINESS_CHECK: (
        ResourceReadinessCheckRequest,
        ResourceReadinessCheckResult,
    ),
}
REQUEST_CLASS_TO_EXECUTION_TYPE = {v[0]: k for k, v in EXECUTION_TYPE_TO_DATA_MAP.items()}
RESULT_CLASS_TO_EXECUTION_TYPE = {v[1]: k for k, v in EXECUTION_TYPE_TO_DATA_MAP.items()}


@whitelist_for_serdes
class UserCodeExecutionRequest(
    NamedTuple(
        "_UserCodeExecutionRequest",
        [("execution_type", UserCodeExecutionType), ("data", Any)],
    )
):
    """Represents a request to execute user code in the GRPC server.

    This is a generic wrapper around the specific request types, existing to prevent us from
    having to add a new GRPC method for every new type of request. This is important for Cloud because
    the agent needs to broker requests to the GRPC server, and we don't want to have to require the user
    to bump the agent version every time we add a new request type. Instead, the agent can just shuttle
    the serialized request data through to the GRPC server, which can then unpack it into the correct
    request type.
    """

    def __new__(cls, execution_type: UserCodeExecutionType, data: Any):
        return super(UserCodeExecutionRequest, cls).__new__(
            cls,
            execution_type=check.inst_param(execution_type, "type", UserCodeExecutionType),
            data=data,
        )

    def unpack_as(self, unpack_class: Type[T]) -> T:
        """Unpacks the request data into the given type, verifying that the request kind matches the
        expected type for the given class.
        """
        check.inst(self.data, unpack_class)
        check.invariant(
            unpack_class in REQUEST_CLASS_TO_EXECUTION_TYPE,
            f"Unpack class {unpack_class} does not match any known request type",
        )
        check.invariant(
            REQUEST_CLASS_TO_EXECUTION_TYPE.get(unpack_class) == self.execution_type,
            f"Unpack class {unpack_class} does not match request type {self.execution_type}",
        )
        return self.data


@whitelist_for_serdes
class UserCodeExecutionResult(
    NamedTuple(
        "_UserCodeExecutionResult",
        [("execution_type", UserCodeExecutionType), ("data", Any)],
    )
):
    """Represents the result of executing user code in the GRPC server.

    This is a generic wrapper around the specific result types - see the comment on
    UserCodeExecutionRequest for more details.
    """

    def __new__(cls, execution_type: UserCodeExecutionType, data: Any):
        return super(UserCodeExecutionResult, cls).__new__(
            cls,
            execution_type=check.inst_param(execution_type, "type", UserCodeExecutionType),
            data=data,
        )

    def unpack_as(self, unpack_class: Type[T]) -> T:
        """Unpacks the result data into the given type, verifying that the result kind matches the
        expected type for the given class.
        """
        check.inst(self.data, unpack_class)
        check.invariant(
            unpack_class in RESULT_CLASS_TO_EXECUTION_TYPE,
            f"Unpack class {unpack_class} does not match any known request type",
        )
        check.invariant(
            RESULT_CLASS_TO_EXECUTION_TYPE.get(unpack_class) == self.execution_type,
            f"Unpack class {unpack_class} does not match result type {self.execution_type}",
        )
        return self.data
