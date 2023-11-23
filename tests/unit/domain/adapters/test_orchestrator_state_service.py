import unittest
from unittest.mock import patch

from py_project.domain.adapters.orchestrator_state_service import (
    OrchestratorState,
    ProcessingItem,
)

TESTED_MODULE = "py_project.domain.adapters.orchestrator_state_service"


class TestProcessingItem(unittest.TestCase):
    def setUp(self):
        self.step_name = "IngestionTask"
        self.processing_item = ProcessingItem(step_name=self.step_name, start_time=0.0)

    def tearDown(self) -> None:
        self.processing_item = ProcessingItem(step_name=self.step_name, start_time=0.0)

    def test_step_name(self):
        assert self.processing_item.step_name == self.step_name

    def test_inputs(self):
        assert self.processing_item.inputs == []

    def test_outputs(self):
        assert self.processing_item.outputs == []

    def test_end_time(self):
        assert self.processing_item.end_time is None

    def test_start_time(self):
        assert self.processing_item.start_time == 0.0

    def test_end_time_setter(self):
        # Given
        given_end_time = 1.0

        # When
        self.processing_item.end_time = given_end_time

        # Then
        assert self.processing_item.end_time == given_end_time

    def test_representation(self):
        assert callable(getattr(self.processing_item, "__repr__", None))

    def test_add_inputs(self):
        # Given
        given_inputs_a = ["path/to/file/a"]
        given_inputs_b = ["path/to/file/b"]
        expected_inputs = given_inputs_a + given_inputs_b

        # When
        self.processing_item.add_inputs(given_inputs_a)
        self.processing_item.add_inputs(given_inputs_b)

        # Then
        assert self.processing_item.inputs == expected_inputs

    def test_add_outputs(self):
        # Given
        given_outputs_a = ["path/to/file/a"]
        given_outputs_b = ["path/to/file/b"]
        expected_outputs = given_outputs_a + given_outputs_b

        # When
        self.processing_item.add_outputs(given_outputs_a)
        self.processing_item.add_outputs(given_outputs_b)

        # Then
        assert self.processing_item.outputs == expected_outputs

    @patch(f"{TESTED_MODULE}.time")
    def test_processing_done(self, mock_time):
        mock_time.return_value = 2.0
        self.processing_item.processing_done()
        assert self.processing_item.end_time == 2.0

    @patch(f"{TESTED_MODULE}.logging.warning")
    def test_processing_done_should_warn_if_overriding_end_time(self, logging_mock):
        # Given
        self.processing_item.end_time = 2.0

        # When
        self.processing_item.processing_done()

        # Then
        logging_mock.assert_called_once()

    def test_to_json(self):
        # Given
        given_step_name = "step_name"
        given_start_time = 0.0
        given_end_time = 1.0
        given_inputs = ["input_file"]
        given_outputs = ["output_file"]
        given_posts = {"task_name": {"file_paths": ["path/to_file"]}}
        given_processing_item = ProcessingItem(
            step_name=given_step_name,
            start_time=given_start_time,
            end_time=given_end_time,
            inputs=given_inputs,
            outputs=given_outputs,
        )
        given_processing_item.posts = given_posts
        expected_json_result = (
            '{"stepName": "step_name", "startTime": 0.0, "endTime": 1.0,'
            ' "inputs": ["input_file"], "outputs": ["output_file"],'
            ' "posts": {"task_name": {"file_paths": ["path/to_file"]}}}'
        )
        # When
        json_result = ProcessingItem.to_json(given_processing_item)

        # Then
        assert expected_json_result == json_result

    def test_from_json(self):
        # Given
        expected_step_name = "step_name"
        expected_start_time = 0.0
        expected_end_time = 1.0
        expected_inputs = ["input_file"]
        expected_outputs = ["output_file"]
        expected_posts = {"task_name": {"file_paths": ["path/to_file"]}}
        expected_processing_item = ProcessingItem(
            step_name=expected_step_name,
            start_time=expected_start_time,
            end_time=expected_end_time,
            inputs=expected_inputs,
            outputs=expected_outputs,
        )
        expected_processing_item.posts = expected_posts
        given_json_str = (
            '{"stepName": "step_name", "startTime": 0.0, "endTime": 1.0,'
            ' "inputs": ["input_file"], "outputs": ["output_file"],'
            ' "posts": {"task_name": {"file_paths": ["path/to_file"]}}}'
        )
        # When
        processing_item: ProcessingItem = ProcessingItem.from_json(given_json_str)

        # Then
        assert processing_item.step_name == expected_processing_item.step_name

    def test_extract_payload_file_paths_from_processing_items(self):
        # Given
        given_step_name = "step_name"
        given_start_time = 0.0
        given_end_time = 1.0
        given_inputs = ["input_file"]
        given_outputs = ["output_file"]
        given_post_dest = "given_post_dest"
        given_post_entry = "file_paths"
        given_post_value = ["path/to_file"]
        given_posts = {given_post_dest: {given_post_entry: given_post_value}}
        given_processing_item = ProcessingItem(
            step_name=given_step_name,
            start_time=given_start_time,
            end_time=given_end_time,
            inputs=given_inputs,
            outputs=given_outputs,
        )
        given_processing_item.posts = given_posts
        # When
        result = ProcessingItem.extract_payload_file_paths_from_processing_items(
            processing_items=[given_processing_item], step_name=given_post_dest, post_name=given_post_entry
        )
        # Then
        assert result == given_post_value


class TestOrchestratorState(unittest.TestCase):
    def setUp(self):
        self.execution_time = 0.0
        self.base = "given_base"
        self.mode = "given_mode"
        self.job_id = "given_job_id"
        self.status = "given_status"
        self.orchestrator_state = OrchestratorState(
            execution_time=self.execution_time,
            base=self.base,
            mode=self.mode,
            status=self.status,
            job_id=self.job_id,
        )

    def test_status(self):
        assert self.orchestrator_state.status == self.status

    def test_status_setter(self):
        # Given
        given_status = "given_new_status"
        # When
        self.orchestrator_state.status = given_status
        # Then
        assert self.orchestrator_state.status == given_status

    def test_mode(self):
        assert self.orchestrator_state.mode == self.mode

    def test_base(self):
        assert self.orchestrator_state.base == self.base

    def test_state_processing_items(self):
        assert self.orchestrator_state.state_processing_items == []

    def test_mode_setter(self):
        # Given
        given_mode = "new_given_mode"
        # When
        self.orchestrator_state.mode = given_mode
        # Then
        assert self.orchestrator_state.mode == given_mode

    def test_base_setter(self):
        # Given
        given_base = "new_given_base"
        # When
        self.orchestrator_state.base = given_base
        # Then
        assert self.orchestrator_state.base == given_base

    def test_representation(self):
        assert callable(getattr(self.orchestrator_state, "__repr__", None))

    def test_state_processing_items_setter(self):
        # Given
        given_step_name = "new_given_step_name"
        given_processing_item = ProcessingItem(step_name=given_step_name, start_time=0.0)
        # When
        self.orchestrator_state.state_processing_items = [given_processing_item]
        # Then
        assert self.orchestrator_state.state_processing_items[0].step_name == given_processing_item.step_name

    def test_update_processing_item(self):
        # Given
        given_step_name = "given_step_name"
        given_processing_item = ProcessingItem(step_name=given_step_name, start_time=0.0)
        # When
        self.orchestrator_state.update_processing_item(given_processing_item)
        # Then
        assert self.orchestrator_state.state_processing_items[0].step_name == given_processing_item.step_name

    def test_to_json(self):
        # Given
        given_orchestrator_state = OrchestratorState(
            execution_time=0,
            base="given_base",
            mode="given_mode",
            status="given_status",
            job_id="given_job_id",
        )
        given_orchestrator_state.posts = {"given_recipient": {"given_object": "message"}}
        expected_json_str = (
            '{"executionTime": 0, "base": "given_base", '
            '"mode": "given_mode", "status": "given_status", "stateProcessingItems": [], '
            '"jobId": "given_job_id", "posts": {"given_recipient": {"given_object": "message"}}}'
        )
        # When
        json_str = OrchestratorState.to_json(given_orchestrator_state)
        # Then
        assert json_str == expected_json_str

    def test_from_json(self):
        # Given
        given_json_str = (
            '{"executionTime": 0, "base": "given_base", '
            '"mode": "given_mode", "status": "given_status", "stateProcessingItems": [], '
            '"jobId": "given_job_id", "posts": {"given_recipient": {"given_object": "message"}}}'
        )
        expected_job_id = "given_job_id"
        # When
        orchestrator_state = OrchestratorState.from_json(given_json_str)
        # Then
        assert orchestrator_state.job_id == expected_job_id

    def test_update_state(self):
        # Given
        given_execution_time = 3.0
        given_task = "given_task"
        given_status = "given_status"
        given_step_name = "given_step_name"
        given_processing_item: ProcessingItem = ProcessingItem(step_name=given_step_name, start_time=0.0)
        expected_status = f"{given_task} {given_status}"

        # When
        self.orchestrator_state.update_state(given_execution_time, given_task, given_status, given_processing_item)

        # Then
        assert self.orchestrator_state.execution_time == given_execution_time
        assert self.orchestrator_state.status == expected_status
        assert self.orchestrator_state.state_processing_items[0].step_name == given_step_name
