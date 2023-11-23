from typing import List, Tuple

import pandas as pd

from py_project.config import filesystem_config
from py_project.domain.entities import weather_data_handler
from py_project.domain.entities.file_handler import FileHandler
from py_project.logger import log_memory_percent_usage


@log_memory_percent_usage
def ingest_normalized_inclino_metrics_to_database(source_file_handler: FileHandler, input_file_paths: List[str]):
    filtered_file_paths = input_file_paths
    normalized_metrics_df_to_load, _ = extract_and_transform_raw_files(source_file_handler, filtered_file_paths)

    dest_folder = filesystem_config.APPS_SILVER_NORMALIZED_FOLDER
    dest_filename = filesystem_config.APPS_SILVER_NORMALIZED_FILENAME
    output_file_paths = source_file_handler.write_file_in_folder(
        input_df=normalized_metrics_df_to_load, dest_folder=dest_folder, filename=dest_filename
    )

    del normalized_metrics_df_to_load

    return input_file_paths, output_file_paths


@log_memory_percent_usage
def extract_and_transform_raw_files(file_handler: FileHandler, file_paths: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    df_raw = file_handler.read_files(
        file_paths=file_paths,
        file_type="CSV",
        sep=",",
        encoding="latin1",
        quotechar='"',
        thousands=",",
    )

    metrics_df = validate_and_transform_raw_metrics(df_raw)
    return metrics_df, file_paths


@log_memory_percent_usage
def validate_and_transform_raw_metrics(df_raw: pd.DataFrame) -> pd.DataFrame:
    transformed_df = df_raw.copy()
    if not transformed_df.empty:
        transformed_df = weather_data_handler.transform_to_raw_metrics(transformed_df)
        transformed_df = weather_data_handler.transform_from_raw_to_normalized_metrics(transformed_df)
    return transformed_df
