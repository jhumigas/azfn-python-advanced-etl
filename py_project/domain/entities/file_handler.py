import typing

import pandas as pd

from py_project.domain.adapters.filesystem import FileSystem


class UnimplementReadOperationError(Exception):
    pass


class FileHandler:
    def __init__(self, filesystem: FileSystem):
        self.filesystem = filesystem

    def ls(self, folder_path: str):
        return self.filesystem.ls(folder_path)

    def read_file(self, file_path: str, file_type: str = "CSV", **kwargs):
        if file_type == "CSV":
            return self.filesystem.read_csv(file_path, skiprows=0, **kwargs)
        elif file_type == "PARQUET":
            return self.filesystem.read_parquet(file_path, **kwargs)
        else:
            raise UnimplementReadOperationError(f"Read operation for {file_type} not implemented, use CSV or PARQUET")

    def read_files(self, file_paths: typing.List[str], file_type: str = "CSV", **kwargs) -> pd.DataFrame:
        list_df = []
        if len(file_paths) > 0:
            for file_path in file_paths:
                item_df = self.read_file(file_path=file_path, file_type=file_type, **kwargs)
                list_df.append(item_df.copy(deep=True))
            output_df = pd.concat(list_df).reset_index(drop=True)
            return output_df
        return pd.DataFrame()

    def get_file_paths_in_folder(self, folder_path: str, only_latest_file: bool) -> typing.List[str]:
        filenames = self.ls(folder_path)
        if only_latest_file:
            filenames = [max(filenames)]
        file_paths = [f"{folder_path}/{filename}" for filename in filenames]
        return file_paths

    def write_file(self, input_df: pd.DataFrame, file_path: str, file_type: str = "PARQUET", **kwargs):
        if file_type == "PARQUET":
            return self.filesystem.write_parquet(input_df=input_df, file_path=file_path, **kwargs)
        else:
            raise UnimplementReadOperationError(f"Write operation for {file_type} not implemented, use PARQUET")

    def write_file_in_folder(
        self, input_df: pd.DataFrame, dest_folder: str, filename: str, file_type: str = "PARQUET"
    ) -> typing.List[str]:
        if not self.filesystem.exists(dest_folder):
            self.filesystem.mkdir(dest_folder)

        if not input_df.empty:
            dest_path = f"{dest_folder}/{filename}"
            self.write_file(input_df, file_path=dest_path, file_type=file_type)
            return [dest_path]
        else:
            return []
