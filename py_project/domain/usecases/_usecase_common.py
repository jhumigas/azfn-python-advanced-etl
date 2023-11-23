from typing import List

import pandas as pd

from py_project.config import database_config
from py_project.domain.adapters.database import Database


def load_metrics_to_database(
    database: Database,
    metrics_df: pd.DataFrame,
    table_name: str,
    write_mode: str = database_config.WRITE_MODE_TRUNCATE_THEN_APPEND,
) -> List[str]:
    if not metrics_df.empty:
        database.write_dataframe(
            input_df=metrics_df,
            table_name=table_name,
            schema=database_config.DEFAULT_SCHEMA,
            write_mode=write_mode,
        )

        return [f"{database_config.DEFAULT_DATABASE}/{database_config.DEFAULT_SCHEMA}/{table_name}"]

    else:
        return []


def summarize_batch_to_load(input_df: pd.DataFrame, id_column: str, timestamp_column: str) -> pd.DataFrame:
    if not set([id_column, timestamp_column]).issubset(input_df.columns):
        bounds_df = pd.DataFrame(
            columns=[
                timestamp_column,
                f"{timestamp_column}_min",
                f"{timestamp_column}_max",
            ]
        )
        return bounds_df
    bounds_df: pd.DataFrame = (
        input_df[[timestamp_column, id_column]].groupby(by=id_column).agg({timestamp_column: ["min", "max"]})
    )
    bounds_df.columns = ["_".join(colname) for colname in bounds_df.columns]
    bounds_df = bounds_df.reset_index()
    return bounds_df
