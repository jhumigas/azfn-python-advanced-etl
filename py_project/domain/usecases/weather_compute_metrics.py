from typing import List, Tuple

import pandas as pd

from py_project.config import database_config
from py_project.domain.adapters.database import Database
from py_project.domain.entities import validate_output, weather_data_handler
from py_project.domain.entities.file_handler import FileHandler
from py_project.domain.usecases._usecase_common import load_metrics_to_database


def compute_weather_metrics(apps_file_handler: FileHandler, input_file_paths: List[str], database: Database):
    normalized_metrics_df_to_load, _ = extract_and_transform_weather_normalized_metrics(
        file_handler=apps_file_handler, file_paths=input_file_paths
    )
    outputs = load_metrics_to_database(
        database=database,
        metrics_df=normalized_metrics_df_to_load,
        table_name=database_config.WEATHER_METRICS_TABLE_NAME,
        write_mode=database_config.WRITE_MODE_TRUNCATE_THEN_APPEND,
    )

    del normalized_metrics_df_to_load

    return input_file_paths, outputs


def extract_and_transform_weather_normalized_metrics(
    file_handler: FileHandler, file_paths: List[str]
) -> Tuple[pd.DataFrame, List[str]]:
    df_raw = file_handler.read_files(
        file_paths=file_paths,
        file_type="PARQUET",
    )
    metrics_df = validate_and_transform_weather_normalized_metrics(df_raw)
    return metrics_df, file_paths


def add_computed_columns(metrics_df: pd.DataFrame) -> pd.DataFrame:
    metrics_df[weather_data_handler.WEATHER_WIND_POWER] = 0.0
    return metrics_df


def validate_and_transform_weather_normalized_metrics(
    normalized_metrics_df: pd.DataFrame,
) -> pd.DataFrame:
    transformed_df = normalized_metrics_df.copy()
    if not transformed_df.empty:
        data_validator_fn = validate_output(weather_data_handler.NormalizedWeatherMetricsSchema)(
            lambda input_df: input_df
        )
        transformed_df = data_validator_fn(transformed_df)
        transformed_df = weather_data_handler.transform_from_normalized_to_computed_metrics(
            transformed_df, add_computed_metrics_fn=add_computed_columns
        )
    return transformed_df
