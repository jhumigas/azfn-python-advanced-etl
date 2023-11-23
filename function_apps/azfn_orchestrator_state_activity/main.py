from py_project.domain.adapters.orchestrator_state_service import OrchestratorState
from py_project.infrastructure.functions_orchestrator_state_service import (
    FunctionsOrchestratorStateService,
)
from py_project.logger import logger
from py_project.config import filesystem_config
from py_project.infrastructure.local_filesystem import LocalFileSystem


def main(payload: OrchestratorState) -> OrchestratorState:
    logger.info(payload)
    base_log_config = filesystem_config.ORCHESTRATOR_LOG_MAPPING[payload.base]

    file_system = LocalFileSystem()

    orchestrator_state_service = FunctionsOrchestratorStateService(file_system=file_system)

    log_filename = f"{base_log_config[filesystem_config.ORCHESTRATOR_LOG_FILENAME_KEY]}".replace(
        filesystem_config.DATE_PATTERN, orchestrator_state_service.get_current_utc_date()
    )

    log_folder_path = f"{base_log_config[filesystem_config.ORCHESTRATOR_LOG_FOLDER_KEY]}"
    file_system.mkdir(log_folder_path)
    log_file_path = f"{log_folder_path}/{log_filename}"

    orchestrator_state_service.save_state(payload=payload, log_file_path=log_file_path)

    return payload


if __name__ == "__main__":
    main(OrchestratorState(execution_time=0, mode="", status="", base="WEATHER", job_id=""))
