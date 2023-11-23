import os
from typing import Optional

import pandas as pd

from py_project.domain.adapters.filesystem import FileSystem
from py_project.logger import logging


class LocalFileSystem(FileSystem):
    def __init__(self):
        pass

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def ls(self, folder: str):
        return os.listdir(folder)

    def mkdir(self, folder_name: str):
        if not os.path.exists(folder_name):
            return os.makedirs(folder_name)

    def isdir(self, folder_path: str):
        return os.path.isdir(folder_path)

    def open(self, file_path: str, mode: str):
        return open(file_path, mode)

    def read_csv(self, file_path: str, skiprows: int = 0, **kwargs) -> Optional[pd.DataFrame]:
        logging.info(f"Reading file: {file_path}")
        return pd.read_csv(file_path, skiprows=skiprows, **kwargs)

    def write_parquet(self, input_df: pd.DataFrame, file_path: str, **kwargs):
        logging.info(f"Writing file: {file_path}")
        return input_df.to_parquet(file_path, **kwargs)

    def read_parquet(self, file_path: str, **kwargs):
        logging.info(f"Reading file: {file_path}")
        return pd.read_parquet(file_path, **kwargs)
