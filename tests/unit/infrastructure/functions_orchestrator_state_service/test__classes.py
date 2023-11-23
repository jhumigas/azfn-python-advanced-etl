import functools
import json
import os
import unittest
from datetime import datetime
from typing import Any, List
from unittest.mock import MagicMock, patch

import azure.durable_functions as durable_func
import pytest

from py_project.domain.adapters.orchestrator_state_service import (
    OrchestratorState,
    ProcessingItem,
)
from py_project.infrastructure.functions_orchestrator_state_service import (
    FunctionsOrchestratorBuilder,
    FunctionsOrchestratorStateService,
    FunctionsOrchestratorTaskManager,
)
from py_project.infrastructure.local_filesystem import LocalFileSystem

TESTED_MODULE = "py_project.infrastructure.functions_orchestrator_state_service"


class TestOrchestratorStateService(unittest.TestCase):
    def sample_state_file(self):
        state_file_path = os.path.join((os.path.dirname(os.path.dirname(__file__))), "data/logs/logs_sample.json")
        with open(state_file_path, "r") as f:
            return json.load(f)

    def setUp(self):
        self.data_path = os.path.join((os.path.dirname(os.path.dirname(__file__))), "data/")
        self.state_files_folder_path = os.path.join(self.data_path, "logs/samples")
        self.file_system = LocalFileSystem()
        self.list_states = self.sample_state_file()

    @patch(f"{TESTED_MODULE}._classes.datetime.datetime")
    def test_get_current_utc_date(self, clock_mock):

        # Given
        mock_today_value = datetime(2021, 11, 19, 00, 00, 00)
        clock_mock.today.return_value = mock_today_value
        expected_current_utc_date = mock_today_value.strftime("%Y%m%d")

        # When
        current_utc_date = FunctionsOrchestratorStateService.get_current_utc_date()

        # Then
        assert current_utc_date == expected_current_utc_date

    @patch(f"{TESTED_MODULE}.FunctionsOrchestratorStateService.write_state_file")
    @patch(f"{TESTED_MODULE}.FunctionsOrchestratorStateService.read_state_file")
    @patch(f"{TESTED_MODULE}._classes.LocalFileSystem.exists")
    def test_save_state_when_previous_logs_exist(self, mock_fs_exists, mock_read_state_file, mock_write_state_file):
        # Given
        given_log_file_system: FunctionsOrchestratorStateService = FunctionsOrchestratorStateService(self.file_system)
        given_log_file_path: str = "path/to/log.json"
        given_state: OrchestratorState = OrchestratorState(
            execution_time=0,
            base="",
            mode="",
            status="",
            job_id="",
        )
        given_logs: List[Any] = [{"time": 0}]

        mock_fs_exists.return_value = True
        mock_read_state_file.return_value = given_logs
        mock_write_state_file.return_value = None

        # When
        given_log_file_system.save_state(given_state, given_log_file_path)

        # Then
        mock_fs_exists.assert_called_once()
        mock_read_state_file.assert_called_once()
        mock_write_state_file.assert_called_once()

    @patch(f"{TESTED_MODULE}._classes.LocalFileSystem.open")
    @patch(f"{TESTED_MODULE}._classes.LocalFileSystem.exists")
    def test_write_state_file(self, mock_fs_exists, mock_fs_open):
        # Given
        given_log_file_system: FunctionsOrchestratorStateService = FunctionsOrchestratorStateService(self.file_system)
        given_logs: Any = [{"log": "error"}]

        given_logs_json_path: str = os.path.join(self.data_path, "logs/logs_tmp.json")
        mock_fs_exists.return_value = True
        mock_fs_open.return_value = open(given_logs_json_path, "w")

        # When
        given_log_file_system.write_state_file(json_obj=given_logs, file_path=given_logs_json_path)

        # Then
        assert os.path.exists(given_logs_json_path)
        mock_fs_open.assert_called_once()
        os.remove(given_logs_json_path)

    @patch(f"{TESTED_MODULE}._classes.LocalFileSystem.open")
    @patch(f"{TESTED_MODULE}._classes.LocalFileSystem.exists")
    def test_read_state_file(self, mock_fs_exists, mock_fs_open):
        # Given
        given_log_file_system: FunctionsOrchestratorStateService = FunctionsOrchestratorStateService(self.file_system)

        given_logs_json_path: str = os.path.join(self.data_path, "logs/logs_sample.json")
        mock_fs_exists.return_value = True
        mock_fs_open.return_value = open(given_logs_json_path, "r")

        # When
        given_log_file_system.read_state_file(file_path=given_logs_json_path)

        # Then
        mock_fs_open.assert_called_once()

    @patch(f"{TESTED_MODULE}.FunctionsOrchestratorStateService.write_state_file")
    @patch(f"{TESTED_MODULE}.FunctionsOrchestratorStateService.read_state_file")
    @patch(f"{TESTED_MODULE}._classes.LocalFileSystem.exists")
    def test_save_state_when_no_previous_states(
        self, mock_fs_exists: MagicMock, mock_read_state_file: MagicMock, mock_write_state_file: MagicMock
    ):
        # Given
        given_log_file_system: FunctionsOrchestratorStateService = FunctionsOrchestratorStateService(self.file_system)
        given_log_file_path: str = "path/to/log.json"
        given_state: OrchestratorState = OrchestratorState(
            execution_time=0,
            base="",
            mode="",
            status="",
            job_id="",
        )
        given_logs: List[Any] = [{"time": 0}]

        mock_fs_exists.return_value = False
        mock_read_state_file.return_value = given_logs
        mock_write_state_file.return_value = None

        # When
        given_log_file_system.save_state(given_state, given_log_file_path)

        # Then
        mock_fs_exists.assert_called_once()
        mock_write_state_file.assert_called_once()
        mock_read_state_file.assert_not_called()

    @patch(f"{TESTED_MODULE}.FunctionsOrchestratorStateService.read_state_file")
    @patch(f"{TESTED_MODULE}._classes.LocalFileSystem.ls")
    @patch(f"{TESTED_MODULE}._classes.LocalFileSystem.isdir")
    def test_get_last_state(self, mock_fs_isdir: MagicMock, mock_fs_ls: MagicMock, mock_read_state_file: MagicMock):
        # Given
        given_log_file_system: FunctionsOrchestratorStateService = FunctionsOrchestratorStateService(self.file_system)
        given_state_file_folder_path = MagicMock()

        mock_fs_isdir.return_value = True
        mock_fs_ls.return_value = ["path/to/a", "path/to/b"]
        mock_read_state_file.return_value = self.list_states

        # When
        last_state: OrchestratorState = given_log_file_system.get_last_state(given_state_file_folder_path)

        # Then
        assert last_state.job_id == "1176750ace1542eeab145cdab06690f1"

    @patch(f"{TESTED_MODULE}._classes.LocalFileSystem.open")
    @patch(f"{TESTED_MODULE}._classes.LocalFileSystem.ls")
    @patch(f"{TESTED_MODULE}._classes.LocalFileSystem.isdir")
    def test_get_last_state_given_status(
        self, mock_fs_isdir: MagicMock, mock_fs_ls: MagicMock, mock_fs_open: MagicMock
    ):
        # Given
        given_log_file_system: FunctionsOrchestratorStateService = FunctionsOrchestratorStateService(self.file_system)
        given_state_file_folder_path = self.state_files_folder_path
        given_task = "given_task"
        given_task_status = "TARGET"
        given_status = f"{given_task} {given_task_status}"

        mock_fs_isdir.return_value = True
        mock_fs_ls.side_effect = os.listdir
        mock_fs_open.side_effect = open

        # When
        last_state: OrchestratorState = given_log_file_system.get_last_state_given_status(
            given_state_file_folder_path, given_status
        )

        # Then
        assert last_state.job_id == "given_job_target_id"

    @patch(f"{TESTED_MODULE}.FunctionsOrchestratorStateService.read_state_file")
    def test_get_last_state_given_status_from_state_file(self, mock_read_state_file: MagicMock):
        # Given
        given_log_file_system: FunctionsOrchestratorStateService = FunctionsOrchestratorStateService(self.file_system)
        given_state_file_folder_path = MagicMock()
        given_task = "given_task"
        given_status = f"{given_task} RUNNING"

        mock_read_state_file.return_value = self.list_states

        # When
        last_state: OrchestratorState = given_log_file_system.get_last_state_given_status_from_state_file(
            given_state_file_folder_path, given_status
        )

        # Then
        assert last_state.job_id == "given_job_id"


class TestOrchestratorTaskManager(unittest.TestCase):
    def setUp(self) -> None:
        self.given_execution_time = 0.0
        self.given_base = "given_base"
        self.given_mode = "given_mode"
        self.given_job_id = "given_job_id"
        self.given_status = "given_status"
        self.given_orchestrator_state = OrchestratorState(
            execution_time=self.given_execution_time,
            base=self.given_base,
            mode=self.given_mode,
            status=self.given_status,
            job_id=self.given_job_id,
        )

    @patch(f"{TESTED_MODULE}.FunctionsOrchestratorTaskManager.update_state")
    @patch(f"{TESTED_MODULE}.FunctionsOrchestratorTaskManager.log_state")
    @patch(f"{TESTED_MODULE}.FunctionsOrchestratorTaskManager.do_task")
    def test_execute_task(self, mock_do_task: MagicMock, mock_log_state: MagicMock, mock_update_state: MagicMock):
        # Given
        given_orchestrator_task_manager: FunctionsOrchestratorTaskManager = FunctionsOrchestratorTaskManager(
            orchestrator_context=MagicMock(),
            orchestrator_state=self.given_orchestrator_state,
            task_logger_name="given_logger_name",
        )

        mock_do_task.return_value = None
        mock_log_state.return_value = None
        mock_update_state.return_value = None
        # When
        result = given_orchestrator_task_manager.execute_task(
            task_name="given_task_name", task_payload="given_task_payload"
        )
        # Call two next
        # First for priming
        # Second for execution
        next(result)
        next(result)
        # Then
        mock_do_task.assert_called_once()
        mock_log_state.assert_called_once()
        mock_update_state.assert_called_once()

    @patch(f"{TESTED_MODULE}._classes.logger.exception")
    def test_try_task_decorator(self, logger_mock: MagicMock):
        @FunctionsOrchestratorTaskManager.try_task_decorator
        def method_with_error():
            raise Exception("Dumb Error")

        with pytest.raises(Exception):
            method_with_error()

        logger_mock.assert_called()

    @patch(f"{TESTED_MODULE}._classes.logger.exception")
    def test_try_task_decorator_with_yield(self, logger_mock: MagicMock):
        @FunctionsOrchestratorTaskManager.try_task_decorator
        def method_with_error_with_yield():
            (yield)
            raise Exception("Dumb Error")

        with pytest.raises(Exception):
            co_routine = yield from method_with_error_with_yield()
            next(co_routine)
            next(co_routine)

        logger_mock.assert_called()


class TestOrchestratorBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.given_base_name = "given_base_name"
        self.given_orchestrator_function_name = "given_orchestrator_function_name"
        self.given_pre_ingestion_activity_function_name = "given_pre_ingestion_activity_function_name"
        self.given_ingestion_activities_function_name = ["given_ingestion_activities_function_name"]

    def test_should_build_orchestrator_function(self):
        # Given
        given_orchestrator_builder = FunctionsOrchestratorBuilder(
            base_name=self.given_base_name,
            orchestrator_function_name=self.given_orchestrator_function_name,
            task_logger=self.given_pre_ingestion_activity_function_name,
            task_list=self.given_ingestion_activities_function_name,
        )

        # When
        orchestrator_function = given_orchestrator_builder.build()

        # Then
        assert type(orchestrator_function) is functools.partial

    @patch(f"{TESTED_MODULE}.ProcessingItem.filter_posts_in_processing_items")
    @patch("azure.durable_functions.DurableOrchestrationContext", spec=durable_func.DurableOrchestrationContext)
    @patch(f"{TESTED_MODULE}.FunctionsOrchestratorTaskManager.log_state")
    @patch(f"{TESTED_MODULE}.FunctionsOrchestratorTaskManager.execute_task")
    def test__orchestrator_function(
        self,
        execute_task_mock,
        log_state_mock,
        durable_orchestration_context_mock,
        filter_posts_in_processing_items_mock,
    ):
        def execute_task_side_effect(task_name: str, task_payload: dict):
            result = ProcessingItem(step_name=task_name)
            result.posts = {task_name: "post"}
            yield
            return result

        # fun_orchestrator_task_manager_mock.update_state.side_effect = hello_side_effect
        log_state_mock.return_value = None
        execute_task_mock.side_effect = execute_task_side_effect
        durable_orchestration_context_mock.call_activity.return_value = None
        durable_orchestration_context_mock.current_utc_datetime = datetime(2021, 11, 19, 00, 00, 00)
        filter_posts_in_processing_items_mock.return_value = [{"given_function_name": "post"}]
        # fun_orchestrator_task_manager_mock.call_activity.side_effect = hello_side_effect
        given_base_name = "given_base_name"
        given_orchestrator_function_name = "given_orchestrator_function_name"
        given_state_activity_function_name = "given_state_activity_function_name"
        given_function_name_one = "given_function_name_one"
        given_function_name_two = "given_function_name_two"
        given_ingestion_activities_function_name = [given_function_name_one, given_function_name_two]

        result = FunctionsOrchestratorBuilder._orchestrator_function(
            context=durable_orchestration_context_mock,
            base_name=given_base_name,
            orchestrator_function_name=given_orchestrator_function_name,
            task_logger=given_state_activity_function_name,
            task_list=given_ingestion_activities_function_name,
        )
        output = []
        try:
            while True:
                next(result)
        except StopIteration as e:
            output = e.value
        assert len(output) == len(given_ingestion_activities_function_name)
        output_processing_items: List[ProcessingItem] = list(map(lambda item: ProcessingItem.from_json(item), output))
        assert output_processing_items[0].step_name == given_function_name_one
        assert output_processing_items[1].step_name == given_function_name_two
