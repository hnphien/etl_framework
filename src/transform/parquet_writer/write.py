from pathlib import Path
from typing import List

import fsspec
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet(
    df: pd.DataFrame,
    schema: pa.schema,
    partition_cols: List[str],
    output_path: Path,
    fs: fsspec.filesystem = None,
) -> None:
    """Write a dataframe to a given path on a given filesystem using a given partitioning strategy.

    Args:
        df: The dataframe to write to a Apache Parquet file.
        schema: The schema to apply to the dataframe.
        partition_cols: The desired partioning columns.
        output_path: The output path.
        fs: The filessystem to write
    """
    if len(set(list(df.columns)) - set(schema.names)):
        print(f'[WARNING] Column mismatch {len(set(list(df.columns)))} <> {len(set(schema.names))}')

    table = pa.Table.from_pandas(
        df, schema, preserve_index=False, nthreads=pa.cpu_count(), safe=True
    )

    if partition_cols is None:
        pq.write_table(
            table,
            output_path.as_posix(),
            filesystem=fs,
            compression="snappy",
        )
    else:
        pq.write_to_dataset(
            table,
            output_path.as_posix(),
            partition_cols=partition_cols,
            filesystem=fs,
            compression="snappy",
        )
