from py_project.config import functions_config, Env
from py_project.domain.adapters.orchestrator_state_service import ProcessingItem
from py_project.domain.entities.file_handler import FileHandler
from py_project.infrastructure.postgres_database import PostgresDatabase
from py_project.infrastructure.local_filesystem import LocalFileSystem
from py_project.domain.usecases.weather_compute_metrics import compute_weather_metrics
from py_project.logger import logger


def main(payload: dict) -> ProcessingItem:
    logger.info(f"Started {functions_config.AZFN_TASK_COMPUTE_METRICS_AND_LOAD_TO_DATABASE}")

    processing_item = ProcessingItem(step_name=functions_config.AZFN_TASK_COMPUTE_METRICS_AND_LOAD_TO_DATABASE)
    file_paths = payload.get(functions_config.POST_INPUT_FILE_PATHS_KEY)

    env = Env()
    source_filesystem = LocalFileSystem()

    file_handler = FileHandler(source_filesystem)
    database = PostgresDatabase(env.engine)
    inputs, outputs = compute_weather_metrics(
        apps_file_handler=file_handler,
        input_file_paths=file_paths,
        database=database,
    )

    processing_item.add_inputs(inputs)
    processing_item.add_outputs(outputs)
    processing_item.processing_done()
    processing_item.posts = {}

    return processing_item
