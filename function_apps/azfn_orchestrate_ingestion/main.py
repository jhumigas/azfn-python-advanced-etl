import azure.durable_functions as durable_func

from py_project.config import functions_config
from py_project.infrastructure.functions_orchestrator_state_service import (
    FunctionsOrchestratorBuilder,
)

from py_project.config import base_config

orchestrator_function = FunctionsOrchestratorBuilder(
    base_name=base_config.WEATHER_BASE_NAME,
    orchestrator_function_name=functions_config.AZFN_ORCHESTRATE_INGESTION,
    task_logger=functions_config.AZFN_ORCHESTRATOR_STATE_ACTIVITY,
    task_list=[
        functions_config.AZFN_TASK_PREPARE_INGESTION,
        functions_config.AZFN_TASK_NORMALIZE_METRICS_AND_LOAD_TO_FILESYSTEM,
        functions_config.AZFN_TASK_COMPUTE_METRICS_AND_LOAD_TO_DATABASE,
    ],
).build()

main = durable_func.Orchestrator.create(orchestrator_function)
