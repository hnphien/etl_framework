import json
from pathlib import Path

import pandas as pd


def json_to_dataframe(file_path: Path) -> pd.DataFrame:
    """ Convert json format to pandas dataframe

    Args:
        file_path (Path): path/to/file

    Returns:
        pd.DataFrame: result dataframe to write parquet
    """
    with open(file_path) as json_file:
        data_list = json.load(json_file)

    df = pd.DataFrame(data_list)
    return df
