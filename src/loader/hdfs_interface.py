import subprocess
from pathlib import Path


def upload_file_to_hdfs(source_file: Path, target_hdfs_folder: str):

    hdfs_delete_old_file = f'hdfs dfs -rm -r "{target_hdfs_folder}/*"'
    hdfs_put_command = f'hdfs dfs -put "{source_file}" "{target_hdfs_folder}"'

    try:
        subprocess.call(hdfs_delete_old_file, shell=True)
        subprocess.call(hdfs_put_command, shell=True)
    except Exception as exc:
        print("[ERROR] Failed to load data to HDFS: {}".format(exc))
    return None


def upload_files_in_folder_to_hdfs(source_folder: Path, target_hdfs_folder: str):

    # Get all files in local folder
    files = source_folder.rglob(".*")

    # Loop through all files and upload to HDFS
    for file in files:
        upload_file_to_hdfs(file.absolute(), target_hdfs_folder)

    return None


if __name__ == "__main__":

    source_file = "/home/hhp81hc/data/txt/employee_0.txt"
    target_hdfs = "/data/txt/"

    upload_file_to_hdfs(source_file, target_hdfs)
