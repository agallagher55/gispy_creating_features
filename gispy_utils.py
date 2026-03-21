"""
Utility functions for GIS data processing operations.

Shared helpers for loading features into SDE and replicating features from
RW to RO.

Note: table_to_dataframe and build_field_mapping now live in gispy.utils and
are re-exported here for backward compatibility.
"""

import logging
import os

import arcpy

from gispy.utils import table_to_dataframe, build_field_mapping  # noqa: F401


logger = logging.getLogger(__name__)


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
