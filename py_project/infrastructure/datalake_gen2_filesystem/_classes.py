# coding: utf-8

import logging
import pathlib
from typing import List, Optional

import adlfs
import fsspec
import pandas as pd

from py_project.domain.adapters.filesystem import FileSystem

from ._const import BLOB_STORAGE_PROTOCOL_IMPLEMENTATION_NAME


class DataLakeGen2FileSystem(FileSystem):
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        adls_account_name: str,
        is_adls_connection_anonymous: bool = False,
    ):
        self.file_system_client: adlfs.spec.AzureBlobFileSystem = fsspec.filesystem(
            protocol=BLOB_STORAGE_PROTOCOL_IMPLEMENTATION_NAME,
            account_name=adls_account_name,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            anon=is_adls_connection_anonymous,
        )

    def exists(self, path: str) -> bool:
        return self.file_system_client.exists(path)

    def ls(self, folder: str):
        if folder.startswith("/"):
            folder: str = folder[1:]
        file_paths: List[str] = self.file_system_client.ls(folder, invalidate_cache=True)
        filenames: List[str] = [item[len(folder) :] for item in file_paths]
        corrected_filenames: List[str] = [item[1:] if item.startswith("/") else item for item in filenames]
        return corrected_filenames

    def mkdir(self, folder_name: str):
        empty_file_path = pathlib.Path(folder_name, "keep").as_posix()
        self.file_system_client.touch(empty_file_path)
        self.file_system_client.rm(empty_file_path)
        return self.file_system_client.isdir(folder_name)

    def isdir(self, folder_path: str):
        return self.file_system_client.isdir(folder_path)

    def open(self, file_path: str, mode: str):
        return self.file_system_client.open(file_path, mode)

    def read_csv(self, file_path: str, skiprows: int = 0, **kwargs) -> Optional[pd.DataFrame]:
        does_path_exist = self.exists(file_path)
        if not does_path_exist:
            logging.error(f"File: {file_path} not found!")
            return

        with self.file_system_client.open(file_path, "rb") as f:
            logging.info(f"Reading file: {file_path}!")
            return pd.read_csv(f, skiprows=skiprows, **kwargs)

    def write_parquet(self, input_df: pd.DataFrame, file_path: str, **kwargs):
        parent_folder = pathlib.Path(file_path).parent.as_posix()
        does_parent_folder_exist = self.exists(parent_folder)
        if not does_parent_folder_exist:
            logging.warning(f"File: {file_path} not found!")
            return

        with self.file_system_client.open(file_path, "wb") as f:
            logging.info(f"Writing file: {file_path}!")
            return input_df.to_parquet(f, **kwargs)

    def read_parquet(self, file_path: str, **kwargs):
        does_path_exist = self.exists(file_path)
        if not does_path_exist:
            logging.error(f"File: {file_path} not found!")
            return

        with self.file_system_client.open(file_path, "rb") as f:
            logging.info(f"Reading file: {file_path}!")
            return pd.read_parquet(f, **kwargs)
