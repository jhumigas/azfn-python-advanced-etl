import abc

import pandas as pd


class Database(abc.ABC):
    @abc.abstractclassmethod
    def has_table(self, table_name: str) -> bool:
        pass

    @abc.abstractmethod
    def write_dataframe(self, input_df: pd.DataFrame, table_name: str, schema: str, write_mode: str, **kwargs):
        pass

    @abc.abstractmethod
    def read_dataframe(self, table, **kwargs):
        pass
