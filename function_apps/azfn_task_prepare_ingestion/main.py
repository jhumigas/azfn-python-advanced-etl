from py_project.config import functions_config
from py_project.config import filesystem_config
from py_project.domain.adapters.orchestrator_state_service import ProcessingItem
from py_project.domain.entities.file_handler import FileHandler
from py_project.domain.usecases.prepare_ingestion import get_file_paths_in_folder
from py_project.logger import logger
from py_project.infrastructure.local_filesystem import LocalFileSystem


def main(payload: dict) -> ProcessingItem:
    logger.info(f"Started {functions_config.AZFN_TASK_PREPARE_INGESTION}")
    processing_item = ProcessingItem(step_name=functions_config.AZFN_TASK_PREPARE_INGESTION)

    source_filesystem = LocalFileSystem()

    file_handler = FileHandler(source_filesystem)

    file_paths = get_file_paths_in_folder(
        file_handler=file_handler, folder_path=filesystem_config.APPS_SILVER_RAW_FOLDER
    )

    processing_item = ProcessingItem(step_name=functions_config.AZFN_TASK_PREPARE_INGESTION)

    processing_item.processing_done()
    processing_item.posts = {
        functions_config.AZFN_TASK_NORMALIZE_METRICS_AND_LOAD_TO_FILESYSTEM: {
            functions_config.POST_INPUT_FILE_PATHS_KEY: file_paths
        },
        functions_config.AZFN_ORCHESTRATE_INGESTION: {
            functions_config.POST_SOURCE_PROCESSED_FILE_PATHS_KEY: file_paths
        },
    }

    return processing_item
