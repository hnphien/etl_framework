"""Common functions for extract"""
import json
from pathlib import Path
from typing import List


def write_to_json(data: list[dict], file_path: str) -> bool:
    """writes a list of dictionaries to a json file.

    Args:
        data (list[dict]): list of dictionarries to be written as JSON
        file_path (str): The output file path

    Returns:
        bool: Indicates if writing to the file was successfull or not.
    """
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except Exception as exc:
        print("Failed to write json: {}".format(exc))
        return False