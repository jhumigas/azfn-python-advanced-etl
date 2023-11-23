import abc
from typing import List

import pandas as pd


class FileSystem(abc.ABC):
    @abc.abstractmethod
    def exists(self, file_path: str) -> bool:
        pass

    @abc.abstractmethod
    def ls(self, folder: str) -> List[str]:
        pass

    @abc.abstractmethod
    def mkdir(self, folder_name):
        pass

    @abc.abstractmethod
    def read_csv(self, file_path: str, skiprows: int, **kwargs) -> pd.DataFrame:
        pass

    @abc.abstractmethod
    def write_parquet(self, input_df: pd.DataFrame, file_path: str, **kwargs):
        pass

    @abc.abstractmethod
    def read_parquet(self, file_path: str, **kwargs):
        pass

    @abc.abstractmethod
    def open(file_path: str, mode: str):
        pass
