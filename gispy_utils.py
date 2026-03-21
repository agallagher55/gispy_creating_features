"""
Utility functions for GIS data processing operations.

Shared helpers for converting ArcGIS tables to DataFrames, building field
mappings, loading features into SDE, and replicating features from RW to RO.
"""

import logging
import os

import arcpy
import pandas as pd


logger = logging.getLogger(__name__)


def table_to_dataframe(input_table):
    """
    Convert an ArcGIS table (geodatabase table, feature class, or SDE table)
    to a pandas DataFrame.
    Adapted from Posse_Permits/Scripts/Posse_Permits_Processing.py

    :param input_table: Path to the input table or feature class
    :return: pandas DataFrame containing the table data
    """
    logger.info(f"Converting table '{input_table}' to DataFrame...")

    fields = [
        field.name for field in arcpy.ListFields(input_table)
        if field.type not in ("Geometry", "Blob", "Raster")
    ]

    data = [row for row in arcpy.da.SearchCursor(input_table, fields)]

    df = pd.DataFrame(data, columns=fields)
    logger.info(f"\tConverted {len(df)} rows with {len(fields)} fields")

    print(df.head())

    return df


def build_field_mapping(source, target, field_map_dict):
    """
    Build an arcpy FieldMappings object from a {sde_field: source_column} dict.

    :param source: Path to source dataset (CSV or feature class)
    :param target: Path to target feature class
    :param field_map_dict: Dict of {target_sde_field: source_column_name}
    :return: arcpy.FieldMappings object
    """
    field_mappings = arcpy.FieldMappings()
    field_mappings.addTable(target)

    for target_field, source_field in field_map_dict.items():

        fm_index = field_mappings.findFieldMapIndex(target_field)

        if fm_index == -1:
            logger.warning(
                f"\tField map: target field '{target_field}' not found "
                "in feature class schema. Skipping."
            )
            continue

        fm = field_mappings.getFieldMap(fm_index)
        fm.addInputField(source, source_field)
        field_mappings.replaceFieldMap(fm_index, fm)

    return field_mappings


def load_to_sde(source_feature, target_sde_feature, truncate=True):
    """
    Load located features into the target SDE feature class.

    :param source_feature: Path to source feature class (scratch workspace)
    :param target_sde_feature: Path to target SDE feature class
    :param truncate: If True, delete existing rows before loading
    """
    logger.info(f"Loading features into '{target_sde_feature}'...")

    if truncate:
        row_count = int(arcpy.GetCount_management(target_sde_feature)[0])
        logger.info(f"\tTruncating {row_count} existing rows...")
        arcpy.DeleteRows_management(target_sde_feature)

    with arcpy.EnvManager(preserveGlobalIds=False):
        arcpy.Append_management(
            inputs=source_feature,
            target=target_sde_feature,
            schema_type="NO_TEST",
        )

    new_count = int(arcpy.GetCount_management(target_sde_feature)[0])
    logger.info(f"\tLoaded. New row count: {new_count}")
    logger.info(arcpy.GetMessages())


def replicate_to_ro(rw_sde, ro_sde, feature_names):
    """
    Truncate and load features from RW SDE to RO SDE.

    :param rw_sde: Path to read-write SDE connection
    :param ro_sde: Path to read-only SDE connection
    :param feature_names: List of feature names (e.g.,
        'SDEADM.LND_PPLC_planning_applications')
    """
    logger.info("Replicating features to RO SDE...")

    with arcpy.EnvManager(workspace=ro_sde, preserveGlobalIds=False):
        for feature_name in feature_names:

            ro_feature = os.path.join(ro_sde, feature_name)
            rw_feature = os.path.join(rw_sde, feature_name)

            if not arcpy.Exists(ro_feature):
                logger.warning(
                    f"\tRO feature '{ro_feature}' does not exist. Skipping."
                )
                continue

            logger.info(f"\tTruncating RO feature '{feature_name}'...")
            row_count = int(arcpy.GetCount_management(ro_feature)[0])
            if row_count > 0:
                arcpy.DeleteRows_management(ro_feature)

            logger.info(f"\tLoading from RW to RO...")
            arcpy.Append_management(
                inputs=rw_feature,
                target=ro_feature,
                schema_type="NO_TEST",
            )

            new_count = int(arcpy.GetCount_management(ro_feature)[0])
            logger.info(f"\tRO row count: {new_count}")
