import abc
import json
import logging
import typing
from dataclasses import dataclass
from time import time
from typing import Any, List, Optional, Union


@dataclass
class ProcessingItem:
    def __init__(
        self,
        step_name: str,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
        posts: Optional[dict] = None,
    ):
        self._step_name = step_name
        self._inputs = inputs if inputs is not None else []
        self._outputs = outputs if outputs is not None else []
        self._posts = posts if posts is not None else {}
        self._start_time = start_time if start_time is not None else time()
        self._end_time = end_time

    def __repr__(self):
        return f"{self.__class__.__name__}(stepName={self._step_name})"

    def __dict__(self) -> dict:
        processing_item = {
            "stepName": self.step_name,
            "startTime": self.start_time,
            "endTime": self.end_time,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "posts": self.posts,
        }
        return processing_item

    @property
    def step_name(self) -> str:
        return self._step_name

    @property
    def inputs(self) -> Union[List[str], List[Any]]:
        return self._inputs

    @property
    def outputs(self) -> Union[List[str], List[Any]]:
        return self._outputs

    @property
    def posts(self) -> dict:
        return self._posts

    @posts.setter
    def posts(self, new_posts: dict):
        self._posts = new_posts

    @staticmethod
    def filter_posts_in_processing_items(processing_items: List["ProcessingItem"], step_name: str) -> List[dict]:
        filtered_posts: List[dict] = []
        for processing_item in processing_items:
            if processing_item.posts.get(step_name):
                filtered_posts.append(processing_item.posts[step_name])
        return filtered_posts

    @property
    def start_time(self) -> int:
        return self._start_time

    @property
    def end_time(self) -> int:
        return self._end_time

    @end_time.setter
    def end_time(self, _new_end_time):
        self._end_time = _new_end_time

    def add_inputs(self, inputs: List[str]):
        self._inputs = self._inputs + inputs

    def add_outputs(self, outputs: List[str]):
        self._outputs = self._outputs + outputs

    def processing_done(self):
        if self.end_time is not None:
            logging.warning("End_time overwritten: Beware, this is not a normal behavior.")
        self.end_time = time()

    @staticmethod
    def to_json(processing_item: "ProcessingItem") -> str:
        return json.dumps(processing_item, default=lambda o: o.__dict__())

    @staticmethod
    def from_json(json_str: str) -> "ProcessingItem":
        json_obj = json.loads(json_str)
        return ProcessingItem(
            step_name=json_obj.get("stepName"),
            start_time=json_obj.get("startTime"),
            end_time=json_obj.get("endTime"),
            inputs=json_obj.get("inputs"),
            outputs=json_obj.get("outputs"),
            posts=json_obj.get("posts"),
        )

    @staticmethod
    def extract_payload_file_paths_from_processing_items(
        processing_items: typing.List["ProcessingItem"], step_name: str, post_name: str
    ) -> typing.List[str]:
        file_paths = []
        for posts in ProcessingItem.filter_posts_in_processing_items(
            processing_items=processing_items, step_name=step_name
        ):
            if posts.get(post_name):
                file_paths += posts.get(post_name)
        return file_paths


@dataclass
class OrchestratorState:
    def __init__(
        self,
        execution_time: float,
        base: str,
        status: str,
        mode: str,
        job_id: str,
    ):
        self.execution_time = execution_time
        self.job_id = job_id
        self._mode = mode
        self._base = base
        self._status: str = status
        self._state_processing_items: List[Any] = []
        self._posts: dict = {}

    def __repr__(self):
        return f"{self.__class__.__name__}(jobId={self.job_id})"

    def __dict__(self) -> dict:
        orchestration_state = {
            "executionTime": self.execution_time,
            "base": self.base,
            "mode": self.mode,
            "status": self.status,
            "stateProcessingItems": self.state_processing_items,
            "jobId": self.job_id,
            "posts": self.posts,
        }
        return orchestration_state

    @property
    def posts(self) -> dict:
        return self._posts

    @property
    def status(self) -> str:
        return self._status

    @property
    def base(self) -> str:
        return self._base

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def state_processing_items(self) -> List[Any]:
        return self._state_processing_items

    @status.setter
    def status(self, value):
        self._status = value

    @mode.setter
    def mode(self, value):
        self._mode = value

    @base.setter
    def base(self, value):
        self._base = value

    @state_processing_items.setter
    def state_processing_items(self, value):
        self._state_processing_items = value

    def update_processing_item(self, processing_item: ProcessingItem):
        self._state_processing_items = [processing_item]

    @posts.setter
    def posts(self, value: dict):
        self._posts = value

    @staticmethod
    def to_json(state_object: "OrchestratorState") -> str:
        return json.dumps(state_object, default=lambda o: o.__dict__())

    @staticmethod
    def from_json(json_str: str):
        state_dict = json.loads(json_str)
        state_obj = OrchestratorState(
            execution_time=state_dict.get("executionTime"),
            base=state_dict.get("base"),
            mode=state_dict.get("mode"),
            status=state_dict.get("status"),
            job_id=state_dict.get("jobId"),
        )
        state_obj.state_processing_items = list(
            map(
                lambda stateProcessingItem: ProcessingItem.from_json(json.dumps(stateProcessingItem)),
                state_dict.get("stateProcessingItems"),
            )
        )
        state_obj.posts = state_dict.get("posts")
        return state_obj

    def update_state(
        self, execution_time: float, task: str, status: str, processing_item: typing.Optional[ProcessingItem]
    ) -> None:
        self.execution_time = execution_time
        self.status = f"{task} {status}"
        if processing_item is not None:
            self.update_processing_item(processing_item)


class OrchestratorStateService(abc.ABC):
    @abc.abstractmethod
    def save_state(self, payload: OrchestratorState) -> bool:
        pass

    @abc.abstractmethod
    def get_last_state(self) -> Optional[OrchestratorState]:
        pass

    @abc.abstractmethod
    def read_state_file(self, file_path: str) -> List[dict]:
        pass

    @abc.abstractmethod
    def write_state_file(self, state_obj: Any, file_path: str) -> bool:
        pass


class OrchestratorTaskManager(abc.ABC):
    @abc.abstractmethod
    def update_state(
        self, execution_time: float, task: str, status: str, processing_item: typing.Optional[ProcessingItem]
    ):
        pass

    @abc.abstractmethod
    def log_state(self) -> OrchestratorState:
        pass

    @abc.abstractmethod
    def do_task(self, task_name: str, task_payload: dict) -> ProcessingItem:
        pass

    @abc.abstractmethod
    def execute_task(self, task_name: str, task_payload: dict) -> ProcessingItem:
        pass
