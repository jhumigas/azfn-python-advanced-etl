import logging
from typing import Optional

import pandas as pd
from psycopg2 import sql
from psycopg2.sql import Composed
from sqlalchemy import MetaData, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from py_project.config.database_config import (
    WRITE_MODE_APPEND,
    WRITE_MODE_REPLACE,
    WRITE_MODE_TRUNCATE_THEN_APPEND,
    WRITE_MODE_UPSERT,
)
from py_project.domain.adapters.database import Database

from ._exceptions import PostgresError

CHUNKSIZE: int = 10000


def convert_composable_to_string(seq: Composed) -> str:
    parts = str(seq).split("'")
    return "".join([p for i, p in enumerate(parts) if i % 2 == 1])


class PostgresDatabase(Database):
    def __init__(self, engine: Engine):
        self.engine = engine

    def has_table(self, table_name: str, schema_name: str) -> bool:
        return self.engine.has_table(table_name=table_name, schema=schema_name)

    def upsert_maker(self, schema):

        meta = MetaData(bind=self.engine, schema=schema)
        meta.reflect()

        def _upsert(table, conn, keys, data_iter):
            upsert_args = {
                "index_elements": [
                    column.name for column in meta.tables[f"{meta.schema}.{table.name}"].columns if column.primary_key
                ]
            }
            for data in data_iter:
                data = {k: data[i] for i, k in enumerate(keys)}
                upsert_args["set_"] = data
                insert_stmt = insert(meta.tables[f"{meta.schema}.{table.name}"]).values(**data)
                upsert_stmt = insert_stmt.on_conflict_do_update(**upsert_args)
                conn.execute(upsert_stmt)

        return _upsert

    def write_dataframe(self, input_df: pd.DataFrame, schema: str, table_name: str, write_mode: str, **kwargs):
        logging.info(f"Begin Writing to database: Schema:{schema}, Tablename:{table_name} in write_mode: {write_mode}!")
        if write_mode in [WRITE_MODE_REPLACE, WRITE_MODE_APPEND, "fail"]:
            return input_df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                index=False,
                method="multi",
                if_exists=write_mode,
                chunksize=CHUNKSIZE,
                **kwargs,
            )
        elif write_mode == WRITE_MODE_UPSERT:
            upsert_method = self.upsert_maker(schema=schema)
            input_df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                index=False,
                method=upsert_method,
                if_exists=WRITE_MODE_APPEND,
                chunksize=CHUNKSIZE,
                **kwargs,
            )
        elif write_mode == WRITE_MODE_TRUNCATE_THEN_APPEND:
            truncate_statement = text(f"TRUNCATE TABLE {schema}.{table_name};")
            self.engine.execute(truncate_statement)
            return input_df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                index=False,
                method="multi",
                if_exists=WRITE_MODE_APPEND,
                chunksize=CHUNKSIZE,
                **kwargs,
            )
        logging.info(f"End Writing: Schema:{schema}, Tablename:{table_name} in write_mode: {write_mode}!")

    def read_dataframe(self, schema_name: str, table_name: str, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        if query is None:
            if not self.has_table(table_name, schema_name):
                raise PostgresError(f"Table '{schema_name}.{table_name}' doesn't exist")
            composable = sql.SQL(""" SELECT * FROM {schema_name}.{table_name};""").format(
                schema_name=sql.Literal(schema_name),
                table_name=sql.Literal(table_name),
            )
            query = convert_composable_to_string(composable)
        table_pdf = pd.read_sql(query, self.engine, **kwargs)
        return table_pdf
