import argparse
from configparser import ConfigParser
from logging import Logger
from pathlib import Path

from src.config.logging_standard import get_logger
from src.db_interaction.db_interface import TalbeResources
from src.extract.file_extract import extract
from src.extract.utils import write_to_json
from src.transform.transfomer import transform_file


def run(config: ConfigParser, log: Logger, args) -> None:
    """ Run program

    Args:
        config (ConfigParser): configs

    Returns:
        None
    """
    log.warning("Start ETL")

    """EXTRACT
    """
    # Create database session
    db_interact = TalbeResources(conn="sqlite:///etl_db.db", table_name="ETL_TRACKING")

    files_already_created = db_interact.get_all_files()

    input_folder = config["Sources"]["InputFolder"]
    input_folder = Path(input_folder)

    # Get metadata of files
    files_metadata = extract(input_folder, files_already_created, args.extension)
    # Write metadata to json file
    write_to_json(data=files_metadata, file_path="data/metadata.json")
    # Upload to database
    db_interact.add_multiple_records(new_data_list=files_metadata)

    """TRANSFORM
    """
    output_data = config["Sources"]["OutputFolder"]
    for metadata in files_metadata:
        transform_file(metadata, Path(output_data), ["color"])


if __name__ == "__main__":
    # Get config
    config = ConfigParser()
    config.read('config.ini')

    # Get logging
    log = get_logger()

    # Get arguments from CMD
    parser = argparse.ArgumentParser()
    parser.add_argument("--extension", type=str, help="Extension of the source files")
    args = parser.parse_args()
    assert (args.extension is not None), "passed extensions are not valid file extensions."

    run(config, log, args)
