from py_project.config import functions_config
from py_project.domain.adapters.orchestrator_state_service import ProcessingItem
from py_project.domain.entities.file_handler import FileHandler
from py_project.domain.usecases.weather_normalize_metrics import ingest_normalized_inclino_metrics_to_database
from py_project.infrastructure.local_filesystem import LocalFileSystem
from py_project.logger import logger


def main(payload: dict) -> ProcessingItem:
    logger.info(f"Started {functions_config.AZFN_TASK_NORMALIZE_METRICS_AND_LOAD_TO_FILESYSTEM}")

    processing_item = ProcessingItem(step_name=functions_config.AZFN_TASK_NORMALIZE_METRICS_AND_LOAD_TO_FILESYSTEM)

    source_filesystem = LocalFileSystem()

    file_handler = FileHandler(source_filesystem)
    file_paths = payload.get(functions_config.POST_INPUT_FILE_PATHS_KEY)
    inputs, outputs = ingest_normalized_inclino_metrics_to_database(
        source_file_handler=file_handler,
        input_file_paths=file_paths,
    )

    processing_item.add_inputs(inputs)
    processing_item.add_outputs(outputs)
    processing_item.processing_done()
    processing_item.posts = {
        functions_config.AZFN_TASK_COMPUTE_METRICS_AND_LOAD_TO_DATABASE: {
            functions_config.POST_INPUT_FILE_PATHS_KEY: outputs
        },
    }

    return processing_item


if __name__ == "__main__":
    main({functions_config.POST_INPUT_FILE_PATHS_KEY: ["./docker/data/weather_history.csv"]})
