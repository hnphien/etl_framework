from pathlib import Path

import pandas as pd


def csv_to_dataframe(file_path: Path) -> pd.DataFrame:
    """ Convert csv format to pandas dataframe

    Args:
        file_path (Path): path/to/file

    Returns:
        pd.DataFrame: result dataframe to write parquet
    """
    df = pd.read_csv(file_path)
    return df
