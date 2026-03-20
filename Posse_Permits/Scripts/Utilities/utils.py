import arcpy
import os
import logging

import pandas as pd


def with_msgs(command):
    print('-' * 100)
    command
    print(arcpy.GetMessages(0))
    print('-' * 100)


def create_fgdb(out_folder_path, out_name="scratch.gdb"):
    """
    Create scratch workspace (gdb)

    :param out_folder_path:
    :param out_name:
    :return: path to file geodatabase
    """

    print(f"\nCreating File Geodatabase '{out_name}'...")
    workspace_path = os.path.join(out_folder_path, out_name)

    if arcpy.Exists(workspace_path):
        arcpy.Delete_management(workspace_path)

    fgdb = arcpy.CreateFileGDB_management(out_folder_path, out_name).getOutput(0)
    print("\tFile Geodatabase created!")

    return fgdb


def setupLog(fileName):
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s: %(message)s', datefmt='%m-%d-%Y %H:%M:%S')

    handler = logging.FileHandler(fileName)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger


def remove_duplicates_from_csv(csv_file):
    print(f"\nRemoving duplicates from '{csv_file}'...")

    df = pd.read_csv(csv_file)
    og_row_count = df.shape[0]

    df_no_dups = df.drop_duplicates()
    new_row_count = df_no_dups.shape[0]

    rows_removed = og_row_count - new_row_count
    print(f"\tRemoved {rows_removed} rows.")

    df_no_dups.to_csv(csv_file, index=False)

    return csv_file, rows_removed


if __name__ == "__main__":
    dup_csv = r"E:\HRM\Scripts\Python3\Posse_Permits\scripts\exports\PPLC_Public_Works_ROW_Permits_duplicate_records.csv"
    cleaned_feature = remove_duplicates_from_csv(dup_csv)