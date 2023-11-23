from typing import List

from py_project.domain.entities.file_handler import FileHandler


def get_file_paths_in_folder(file_handler: FileHandler, folder_path: str) -> List[str]:
    file_paths = file_handler.get_file_paths_in_folder(folder_path=folder_path, only_latest_file=False)
    return file_paths
