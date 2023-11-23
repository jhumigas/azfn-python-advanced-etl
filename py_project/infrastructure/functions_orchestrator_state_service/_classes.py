import datetime
import functools
import json
import typing
from typing import Any, Callable, Generator, List, Optional, Union

import azure.durable_functions as durable_func

from py_project.domain.adapters.orchestrator_state_service import (
    OrchestratorState,
    OrchestratorStateService,
    OrchestratorTaskManager,
    ProcessingItem,
)
from py_project.infrastructure.local_filesystem import LocalFileSystem
from py_project.logger import logger

from ._const import (
    PAYLOAD_BASE_NAME_KEY,
    PAYLOAD_INGESTION_MODE_KEY,
    PAYLOAD_INPUT_FILE_PATHS_KEY,
    POST_INPUT_FILE_PATHS_KEY,
    POST_SOURCE_PROCESSED_FILE_PATHS_KEY,
    TASK_STATUS_COMPLETED,
    TASK_STATUS_RUNNING,
)


class FunctionsOrchestratorStateService(OrchestratorStateService):
    def __init__(
        self,
        file_system: LocalFileSystem,
    ):
        self.file_system = file_system

    @staticmethod
    def get_current_utc_date() -> str:
        return datetime.datetime.today().strftime("%Y%m%d")

    def read_state_file(self, file_path: str) -> List[dict]:
        with self.file_system.open(file_path, "r") as f:
            json_obj = json.load(f)
            return json_obj

    def write_state_file(self, json_obj: Any, file_path: str) -> bool:
        with self.file_system.open(file_path, "w") as f:
            json.dump(json_obj, f)
            return True

    def save_state(self, payload: OrchestratorState, log_file_path: str):
        jsons_to_add = [json.loads(OrchestratorState.to_json(payload))]
        if self.file_system.exists(log_file_path):
            logs_json: List[Any] = self.read_state_file(log_file_path)
            logs_json += jsons_to_add
            self.write_state_file(logs_json, log_file_path)
        else:
            self.write_state_file(jsons_to_add, log_file_path)

    def get_last_state(self, log_folder_path: str) -> Optional[OrchestratorState]:
        if self.file_system.isdir(log_folder_path):
            last_log_filename = max(self.file_system.ls(log_folder_path))
            last_log_file_path = f"{log_folder_path}/{last_log_filename}"
            last_state_dicts = self.read_state_file(last_log_file_path)
            if len(last_state_dicts) > 0:
                last_state_dict: dict = last_state_dicts[-1]
                last_state: OrchestratorState = OrchestratorState.from_json(json.dumps(last_state_dict))
                return last_state
            return None
        return None

    def get_last_state_given_status(self, log_folder_path: str, status: str) -> Optional[OrchestratorState]:
        if self.file_system.isdir(log_folder_path):
            state_filenames = sorted(self.file_system.ls(log_folder_path), reverse=True)
            for state_filename in state_filenames:
                state_file_path = f"{log_folder_path}/{state_filename}"
                last_state = self.get_last_state_given_status_from_state_file(
                    state_file_path=state_file_path, status=status
                )
                if last_state is not None:
                    return last_state
        return None

    def get_last_state_given_status_from_state_file(
        self, state_file_path: str, status: str
    ) -> Optional[OrchestratorState]:
        last_state_dicts: List[dict] = self.read_state_file(state_file_path)
        filtered_last_state_dicts: List[dict] = [
            state_dict for state_dict in last_state_dicts if state_dict.get("status") == status
        ]
        if len(filtered_last_state_dicts) > 0:
            last_state_dict = filtered_last_state_dicts[-1]
            last_state: OrchestratorState = OrchestratorState.from_json(json.dumps(last_state_dict))
            return last_state
        return None


class FunctionsOrchestratorTaskManager(OrchestratorTaskManager):
    def __init__(
        self,
        orchestrator_context: durable_func.DurableOrchestrationContext,
        orchestrator_state: OrchestratorState,
        task_logger_name: str,
    ):
        self.orchestrator_context = orchestrator_context
        self.task_logger_name = task_logger_name
        self.orchestrator_state = orchestrator_state

    def update_state(self, task: str, status: str, processing_item: Optional[ProcessingItem]):
        self.orchestrator_state.update_state(
            execution_time=datetime.datetime.timestamp(self.orchestrator_context.current_utc_datetime),
            task=task,
            status=status,
            processing_item=processing_item,
        )

    def try_task_decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.exception(f"FAILED TASK ERROR: {e}")
                raise

        return wrapper

    @try_task_decorator
    def log_state(self) -> OrchestratorState:
        return self.orchestrator_context.call_activity(name=self.task_logger_name, input_=self.orchestrator_state)

    @try_task_decorator
    def do_task(self, task_name: str, task_payload: dict) -> ProcessingItem:
        return self.orchestrator_context.call_activity(name=task_name, input_=task_payload)

    def execute_task(
        self, task_name: str, task_payload: dict
    ) -> Generator[Union[OrchestratorState, ProcessingItem], None, ProcessingItem]:
        self.update_state(
            task=task_name,
            status=TASK_STATUS_RUNNING,
            processing_item=None,
        )
        yield self.log_state()
        processing_result: ProcessingItem = yield self.do_task(task_name=task_name, task_payload=task_payload)
        self.update_state(
            task=task_name,
            status=TASK_STATUS_COMPLETED,
            processing_item=processing_result,
        )
        yield self.log_state()

        return processing_result


class FunctionsOrchestratorBuilder:
    def __init__(
        self,
        base_name: str,
        orchestrator_function_name: str,
        task_logger: str,
        task_list: typing.List[str],
    ):
        self.base_name = base_name
        self.orchestrator_function_name = orchestrator_function_name
        self.task_logger = task_logger
        self.task_list = task_list

    @staticmethod
    def _orchestrator_function(
        context: durable_func.DurableOrchestrationContext,
        base_name: str,
        orchestrator_function_name: str,
        task_logger: str,
        task_list: typing.List[str],
    ) -> str:
        ingestion_mode = context.get_input()[PAYLOAD_INGESTION_MODE_KEY]
        # Set initial payload
        activity_task_payload = context.get_input()
        activity_task_payload[PAYLOAD_BASE_NAME_KEY] = base_name
        activity_task_payload[PAYLOAD_INPUT_FILE_PATHS_KEY] = []

        # Init Orchestrator State
        orchestration_state = OrchestratorState(
            execution_time=datetime.datetime.timestamp(context.current_utc_datetime),
            base=base_name,
            mode=ingestion_mode,
            job_id=context.instance_id,
            status="",
        )

        # Init OrchestratorTaskManager
        orchestration_manager: FunctionsOrchestratorTaskManager = FunctionsOrchestratorTaskManager(
            orchestrator_context=context,
            orchestrator_state=orchestration_state,
            task_logger_name=task_logger,
        )

        # Update orchestrator function state to running
        orchestration_manager.update_state(
            task=orchestrator_function_name,
            status=TASK_STATUS_RUNNING,
            processing_item=None,
        )
        yield orchestration_manager.log_state()

        # Init ProcessingItem list
        results: typing.List[ProcessingItem] = []

        # Prepare Chained Tasks
        task_list = task_list

        # Execute Chained Tasks
        for task in task_list:
            activity_task_payload[
                PAYLOAD_INPUT_FILE_PATHS_KEY
            ] = ProcessingItem.extract_payload_file_paths_from_processing_items(
                results, task, POST_INPUT_FILE_PATHS_KEY
            )
            result: ProcessingItem = yield from orchestration_manager.execute_task(
                task_name=task, task_payload=activity_task_payload
            )
            results.append(result)

        # Get Checkmark for orchestration and add it to  Orchestrator posts attribute
        last_processed_file_paths = ProcessingItem.extract_payload_file_paths_from_processing_items(
            results, orchestrator_function_name, POST_SOURCE_PROCESSED_FILE_PATHS_KEY
        )
        orchestration_manager.orchestrator_state.posts = {
            orchestrator_function_name: {POST_SOURCE_PROCESSED_FILE_PATHS_KEY: last_processed_file_paths}
        }
        # update_state_to_done
        orchestration_manager.update_state(
            task=orchestrator_function_name,
            status=TASK_STATUS_COMPLETED,
            processing_item=None,
        )
        yield orchestration_manager.log_state()

        return list(map(lambda item: ProcessingItem.to_json(item), results))

    def build(self) -> typing.Callable[[durable_func.DurableOrchestrationContext], str]:
        return functools.partial(
            self._orchestrator_function,
            base_name=self.base_name,
            orchestrator_function_name=self.orchestrator_function_name,
            task_logger=self.task_logger,
            task_list=self.task_list,
        )
