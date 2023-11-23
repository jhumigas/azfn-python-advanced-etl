import datetime
import unittest

import pandas as pd
import testing.postgresql
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from py_project.config.database_config import (
    WRITE_MODE_APPEND,
    WRITE_MODE_TRUNCATE_THEN_APPEND,
    WRITE_MODE_UPSERT,
)
from py_project.infrastructure.postgres_database import PostgresDatabase
from py_project.infrastructure.postgres_database._exceptions import PostgresError

TESTED_MODULE = "py_project.infrastructure.postgres_database"


class TestPostgresDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.postgresql = testing.postgresql.Postgresql(port=7654)
        connection_string = cls.postgresql.url()
        cls.engine: Engine = create_engine(connection_string)

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls.postgresql.stop()
        except Exception as e:
            print(str(e))
            return None

    def setUp(self) -> None:
        self.schema = "schema_name"
        self.engine.execute(f"CREATE SCHEMA {self.schema};")

    def tearDown(self) -> None:
        self.engine.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        self.engine.execute(f"DROP SCHEMA {self.schema} CASCADE;")

    def test_has_table(self):
        # Given
        given_postgres_database: PostgresDatabase = PostgresDatabase(engine=self.engine)
        given_present_table: str = "present_table"
        given_absent_table: str = "absent_table"
        given_schema: str = self.schema

        # When
        pd.DataFrame().to_sql(name=given_present_table, schema=given_schema, con=self.engine)

        # Then
        self.assertTrue(given_postgres_database.has_table(table_name=given_present_table, schema_name=given_schema))
        self.assertFalse(given_postgres_database.has_table(table_name=given_absent_table, schema_name=given_schema))

    def test_write_dataframe_with_write_mode_append(self):
        # Given
        given_date_col: str = "datetime"
        given_table_name: str = "table_name"
        given_schema: str = self.schema
        row1: pd.Series = pd.Series({given_date_col: datetime.datetime(2020, 1, 1)})
        row2: pd.Series = pd.Series({given_date_col: datetime.datetime(2020, 1, 2)})
        given_df: pd.DataFrame = pd.DataFrame([row1, row2])
        given_postgres_database: PostgresDatabase = PostgresDatabase(engine=self.engine)
        expected_df: pd.DataFrame = pd.DataFrame([row1, row2])

        # When
        given_postgres_database.write_dataframe(
            input_df=given_df, table_name=given_table_name, schema=given_schema, write_mode=WRITE_MODE_APPEND
        )

        # Then
        df_in_db: pd.DataFrame = pd.read_sql(f"SELECT * FROM {given_schema}.{given_table_name}", con=self.engine)
        pd.testing.assert_frame_equal(expected_df, df_in_db)

    def test_write_dataframe_with_write_mode_upsert(self):
        # Given
        given_pk_col: str = "id"
        given_date_col: str = "datetime"
        given_table_name: str = "table_name"
        given_schema: str = self.schema
        self.engine.execute(
            f"CREATE TABLE {given_schema}.{given_table_name}\
                ({given_pk_col} INTEGER PRIMARY KEY, {given_date_col} TIMESTAMP)"
        )
        row1: pd.Series = pd.Series({given_pk_col: 1, given_date_col: datetime.datetime(2020, 1, 1)})
        given_init_df: pd.DataFrame = pd.DataFrame([row1])
        given_init_df.to_sql(
            name=given_table_name, schema=given_schema, con=self.engine, if_exists=WRITE_MODE_APPEND, index=False
        )
        row2: pd.Series = pd.Series({given_pk_col: 2, given_date_col: datetime.datetime(2020, 1, 2)})
        row3: pd.Series = pd.Series({given_pk_col: 1, given_date_col: datetime.datetime(2020, 1, 10)})
        given_df: pd.DataFrame = pd.DataFrame([row3, row2])
        given_postgres_database: PostgresDatabase = PostgresDatabase(engine=self.engine)
        expected_df: pd.DataFrame = pd.DataFrame([row3, row2])

        # When
        given_postgres_database.write_dataframe(
            input_df=given_df, table_name=given_table_name, schema=given_schema, write_mode=WRITE_MODE_UPSERT
        )

        # Then
        df_in_db: pd.DataFrame = pd.read_sql(f"SELECT * FROM {given_schema}.{given_table_name}", con=self.engine)
        pd.testing.assert_frame_equal(expected_df, df_in_db)

    def test_write_dataframe_with_write_mode_truncate_then_append(self):
        # Given
        given_pk_col: str = "id"
        given_date_col: str = "datetime"
        given_table_name: str = "table_name"
        given_schema: str = self.schema
        self.engine.execute(
            f"CREATE TABLE {given_schema}.{given_table_name}\
                ({given_pk_col} INTEGER PRIMARY KEY, {given_date_col} TIMESTAMP)"
        )
        row1: pd.Series = pd.Series({given_pk_col: 1, given_date_col: datetime.datetime(2020, 1, 1)})
        given_init_df: pd.DataFrame = pd.DataFrame([row1])
        given_init_df.to_sql(
            name=given_table_name, schema=given_schema, con=self.engine, if_exists=WRITE_MODE_APPEND, index=False
        )
        row2: pd.Series = pd.Series({given_pk_col: 2, given_date_col: datetime.datetime(2020, 1, 2)})
        row3: pd.Series = pd.Series({given_pk_col: 1, given_date_col: datetime.datetime(2020, 1, 10)})
        given_df: pd.DataFrame = pd.DataFrame([row3, row2])
        given_postgres_database: PostgresDatabase = PostgresDatabase(engine=self.engine)
        expected_df: pd.DataFrame = pd.DataFrame([row3, row2])

        # When
        given_postgres_database.write_dataframe(
            input_df=given_df,
            table_name=given_table_name,
            schema=given_schema,
            write_mode=WRITE_MODE_TRUNCATE_THEN_APPEND,
        )

        # Then
        df_in_db: pd.DataFrame = pd.read_sql(f"SELECT * FROM {given_schema}.{given_table_name}", con=self.engine)
        pd.testing.assert_frame_equal(expected_df, df_in_db)

    def test_read_dataframe(self):
        # Given
        given_postgres_database: PostgresDatabase = PostgresDatabase(engine=self.engine)
        given_schema_name = "schema_name"
        given_table_name = "table_name"

        expected_dataframe = pd.DataFrame({"column": ["value"]})

        expected_dataframe.to_sql(name=given_table_name, schema=given_schema_name, con=self.engine, index=False)

        # When
        dataframe = given_postgres_database.read_dataframe(given_schema_name, given_table_name)

        # Then
        pd.testing.assert_frame_equal(dataframe, expected_dataframe)

    def test_read_dataframe_should_revert_if_table_dont_exist(self):
        # Given
        given_postgres_database: PostgresDatabase = PostgresDatabase(engine=self.engine)
        given_schema_name = "schema_name"
        given_table_name = "no_present_table_name"

        # When
        with self.assertRaises(PostgresError):
            given_postgres_database.read_dataframe(given_schema_name, given_table_name)
