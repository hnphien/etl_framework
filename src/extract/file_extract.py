from pathlib import Path
from typing import List


def extract(root_folder: Path, files_already_created: list, extension: str) -> List[dict]:
    """ Get metadata of files

    Args:
        root_folder (Path): path/to/folder

    Returns:
        List[dict]: [{filename: "", filesize: ""}, {filename: "", filesize: ""}]
    """
    # Get list files in root folder
    files = root_folder.rglob("*.{}".format(extension))

    files_metadata = []

    for file in files:
        file_name = file.name
        file_root = str(file.parent)
        file_size = file.stat().st_size
        file_creation_date = file.stat().st_ctime
        file_extension = file.suffix

        metadata = {
            "file_name": file_name,
            "file_root": file_root,
            "file_size": file_size,
            "file_creation_date": file_creation_date,
            "file_extension": file_extension
        }

        # Check if metadata already created in the tracking database
        # file_path = file_root + "\\" + file_name
        # if file_path not in files_already_created:
        files_metadata.append(metadata)

    return files_metadata
