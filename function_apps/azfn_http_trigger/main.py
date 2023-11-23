from uuid import uuid1

import azure.durable_functions as df
import azure.functions as func

from py_project.config import functions_config
from py_project.logger import logger


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    request_id = str(uuid1())
    instance_id = await client.start_new(
        orchestration_function_name=functions_config.AZFN_ORCHESTRATE_INGESTION,
        client_input={
            functions_config.PAYLOAD_INGESTION_MODE_KEY: functions_config.FULL_INGESTION_MODE,
            functions_config.PAYLOAD_REQUEST_ID_KEY: request_id,
        },
        instance_id=None,
    )
    logger.info(f"Started orchestration with ID = {instance_id}")

    return client.create_check_status_response(req, instance_id)
