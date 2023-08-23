"""Transformer.py"""
from pathlib import Path

from src.transform.extensions.csv_transformer import csv_to_dataframe
from src.transform.extensions.json_transformer import json_to_dataframe
from src.transform.parquet_writer.write import write_parquet
from src.transform.parquet_writer.schemas import schema_color_table


def transform_file(metadata_file: dict, output_data: Path, partitions: list) -> None:
    """ Transform source file
    - If file extension is .csv then call function from csv_transformer.py
    - If file extension is .csv then call function from json_transformer.py
    - If file extension is .csv then call function from xml_transformer.py

    Args:
        file_path (Path): Path(path/to/file)

    Returns:
        None
    """
    # Get path/extension
    file_extension = metadata_file["file_extension"]
    file_path = Path("{}\\{}".format(metadata_file["file_root"], metadata_file["file_name"]))

    if file_extension == ".csv":
        # Transform .csv
        csv_df = csv_to_dataframe(file_path)
        result_df = csv_df
    elif file_extension == ".json":
        json_df = json_to_dataframe(file_path)
        result_df = json_df
    elif file_extension == ".xml":
        result_df = None

    # Write pandas dataframe to parquet file
    print(result_df)
    write_parquet(
        df=result_df,
        schema=schema_color_table,
        partition_cols=partitions,
        output_path=output_data,
        fs=None
    )
