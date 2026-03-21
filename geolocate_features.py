"""
Geolocate planning and subdivision application features by PID (Parcel ID).

Reads application data from Data Warehouse staging tables, locates each record
using parcel geometry from LND_parcel_polygon, and loads the results into
existing SDE feature classes via truncate-and-load.

Two geometry modes are supported (controlled by GEOMETRY_MODE):
  - "POLYGON": All parcel polygons for a given application are unioned into a
    single dissolved polygon so the geometry aligns with the application record.
    A planning application may reference multiple parcels via a comma-separated
    PID field.
  - "POINT": The primary (first) PID is used to locate the application at the
    centroid of its associated parcel polygon.

Modeled after: Posse_Permits/Scripts/Posse_Permits.py
"""

import os
import ast
import time
import datetime

import arcpy
import pandas as pd

from configparser import ConfigParser

from gispy.utils import create_fgdb, setupLog, table_to_dataframe
from pid_locator import ParcelLookup, FeatureLocator, GEOMETRY_MODES
from gispy_utils import load_to_sde, replicate_to_ro

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
WORKING_DIR = os.path.dirname(os.path.abspath(__file__))
SCRATCH_DIR = os.path.join(WORKING_DIR, "Scratch")
REPORTS_DIR = os.path.join(WORKING_DIR, "Reports")
EXPORTS_DIR = os.path.join(WORKING_DIR, "Exports")

FILE_NAME = os.path.basename(__file__)
FILE_NAME_BASE = os.path.splitext(FILE_NAME)[0]

config = ConfigParser()
config.read("config.ini")

feature_config = ConfigParser()
feature_config.optionxform = str  # preserve case
feature_config.read("geolocate.ini")

# SDE connections
SDEADM_RW = config.get("SERVER", "dev_rw")
SDEADM_RO = config.get("SERVER", "dev_ro")

# Data Warehouse staging connection
DW_STG = config.get("HRM_DW", "connFileDev")

# Geolocate settings from feature config
DW_SOURCE_TABLES = ast.literal_eval(
    feature_config.get("GEOLOCATE", "dw_source_tables")
)
DEFAULT_PID_FIELD = feature_config.get("GEOLOCATE", "pid_field")
TRUNCATE_AND_LOAD = feature_config.getboolean("GEOLOCATE", "truncate_and_load")

# Geometry mode — controls how application features are located.
# "POLYGON": union all parcel polygons for each application into one polygon.
# "POINT":   place a point at the centroid of the primary (first) PID's parcel.
GEOMETRY_MODE = "POLYGON"

# Per-table PID field overrides; falls back to DEFAULT_PID_FIELD if not listed
PID_FIELD_OVERRIDES = (
    dict(feature_config.items("PID_FIELDS"))
    if feature_config.has_section("PID_FIELDS")
    else {}
)

# Build final per-table PID field map: {dw_table_name: pid_field_name}
PID_FIELD_MAP = {
    dw_table: PID_FIELD_OVERRIDES.get(dw_table, DEFAULT_PID_FIELD)
    for dw_table, _target in DW_SOURCE_TABLES
}

# Per-table field maps for Append_management: {dw_table_name: {sde_field: dw_column}}
# Only needed when DW column names differ from SDE field names.
# Tables not listed here fall back to name-based matching (schema_type="NO_TEST").
TABLE_FIELD_MAPS = {
    "OPENDATA_SOURCE.PPLC_PLANNING_APPLICATIONS": {
        "APP_NUM":    "Planning_App_Num",
        "APP_NAME":   "Application_Name",
        "APP_TYPE":   "Application_Type",
        "APPSCOPE":   "Application_Scope",
        "ADAPPSCOPE": "Additional_Application_Scope",
        "SUBMITDATE": "Submitted_Date",
        "COMPLEDATE": "Completed_Date",
        "APPSTATUS":  "App_Status",
        "DESCRIPACT": "Proposed_Activity_Description",
        "LOCATION":   "Location",
        "PID":        "PIDs",
        "APPROVDATE": "Approved_Date",
        "FINDDATE":   "Findings_Completed_Date",
        "MONMETDATE": "Monitor_Meetings_Completed_Date",
        "RECORDDATE": "Final_Recordation_Completed_Date",
        "PLUSERNAME": "Assigned_Planner_Name",
        "WEBURL":     "Website_URL",
        "GSA_NAME":   "Community",
        "DIST_ID":    "District",
        "CURLANDUSE": "Current_Land_Use",
        "PROPLANDUSE": "Proposed_Land_Use",
        "REGDESIG":   "Regional_Land_Use_Designation",
        "LOCDESIG":   "Local_Land_Use_Designation",
        "PLAN_NAME":  "Community_Plan_Name",
        "ZONE":       "Zoning",
    },
}

# Reference data
LND_PARCEL_POLYGON = os.path.join(
    SDEADM_RW, "SDEADM.LND_parcels", "SDEADM.LND_parcel_polygon"
)
SPATIAL_REFERENCE = arcpy.Describe(
    os.path.join(
        config.get("SERVER", "prod_rw"),
        "SDEADM.LND_hrm_parcel_parks",
        "SDEADM.LND_hrm_park",
    )
).spatialReference

# Logging
log_file = os.path.join(
    WORKING_DIR, "Logs", f"{datetime.date.today()}_{FILE_NAME_BASE}.log"
)
os.makedirs(os.path.dirname(log_file), exist_ok=True)
logger = setupLog(log_file, log_to_console=True)

# ArcPy environment
arcpy.SetLogHistory(False)
arcpy.env.overwriteOutput = True

SEPARATOR = '=' * 60

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------


def generate_report(no_pid_df, unlocated_df, report_path):
    """
    Generate an Excel report of records that could not be located.

    :param no_pid_df: DataFrame of records with no PID
    :param unlocated_df: DataFrame of records with PID but not found in
        parcel layer
    :param report_path: Path to output Excel file
    """
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    logger.info(f"Generating failure report: {report_path}")
    logger.info(f"\tRecords with no PID: {len(no_pid_df)}")
    logger.info(f"\tRecords with unlocated PID: {len(unlocated_df)}")

    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        if not no_pid_df.empty:
            no_pid_df.to_excel(writer, sheet_name="No_PID", index=False)
        if not unlocated_df.empty:
            unlocated_df.to_excel(
                writer, sheet_name="Unlocated_PID", index=False
            )

    logger.info(f"\tReport saved to {report_path}")


def main(
    scratch_workspace, output_rw_sde, dw_stg, dw_source_tables,
    pid_field_map, spatial_reference, exports_dir, reports_dir,
    truncate_and_load=True, geometry_mode="POLYGON"
):
    """
    Main processing function. Loops over configured DW source tables,
    geolocates records by PID, loads into SDE, and writes one failure report
    per table.

    :param scratch_workspace: Path to scratch file geodatabase
    :param output_rw_sde: Path to read-write SDE connection
    :param dw_stg: Path to Data Warehouse staging SDE connection
    :param dw_source_tables: List of (dw_table_name, target_feature_name)
        tuples
    :param pid_field_map: Dict mapping each dw_table_name to its PID field
        name
    :param spatial_reference: ArcPy SpatialReference object for output
        features
    :param exports_dir: Path to directory for CSV exports
    :param reports_dir: Path to directory for per-table failure reports
    :param truncate_and_load: If True, truncate target features before loading
    :param geometry_mode: "POLYGON" to union all parcel polygons per record;
        "POINT" to place a point at the primary PID's parcel centroid
    """
    logger.info(f"OUTPUT WORKSPACE: {output_rw_sde}")
    logger.info(f"DW STAGING: {dw_stg}")

    processed_features = []

    parcel_fc = os.path.join(
        output_rw_sde, "SDEADM.LND_parcels", "SDEADM.LND_parcel_polygon"
    )

    # Build the feature locator once per run — parcel FC, geometry strategy,
    # scratch workspace, and spatial reference are constant across all tables.
    parcel_lookup = ParcelLookup(parcel_fc)
    locator = GEOMETRY_MODES[geometry_mode](parcel_lookup)
    feature_locator = FeatureLocator(
        scratch_workspace=scratch_workspace,
        spatial_reference=spatial_reference,
        exports_dir=exports_dir,
        locator=locator,
    )

    total_tables = len(dw_source_tables)

    for table_count, table_info in enumerate(dw_source_tables, start=1):

        dw_table_name, target_feature_name = table_info
        dw_table_path = os.path.join(dw_stg, dw_table_name)

        target_sde_feature = os.path.join(
            output_rw_sde, target_feature_name
        ).replace("_TEMP", "")

        feature_label = target_feature_name.replace("SDEADM.", "")

        logger.info(f"\n{SEPARATOR}")
        logger.info(
            f"Processing {table_count}/{total_tables}: {feature_label}"
        )
        logger.info(f"{SEPARATOR}")

        # Verify source table exists
        if not arcpy.Exists(dw_table_path):

            logger.error(f"DW source table not found: {dw_table_path}")
            continue

        # Verify target feature exists
        if not arcpy.Exists(target_sde_feature):

            logger.error(
                f"Target SDE feature not found: {target_sde_feature}"
            )
            continue

        # Resolve PID field for this table
        pid_field = pid_field_map.get(dw_table_name)

        if not pid_field:
            logger.error(
                f"No PID field configured for '{dw_table_name}'. Skipping."
            )
            continue

        # Read source data from Data Warehouse
        table_row_count = int(arcpy.management.GetCount(dw_table_path)[0])
        if table_row_count == 0:

            logger.warning(f"No data found in {dw_table_path}. Skipping.")
            continue

        df = table_to_dataframe(dw_table_path)

        # Remove OBJECTID columns
        df_fields = [
            col for col in df.columns if "OBJECTID" not in col.upper()
        ]
        df = df[df_fields]

        # Clean PID field. The value may hold a comma-separated list
        # (e.g. "00130278, 00130286"); the full string is preserved in the
        # SDE attribute and all PIDs are used when building the merged polygon.
        if pid_field in df.columns:

            df[pid_field] = df[pid_field].astype(str).replace(
                {"nan": None, "None": None}
            )

            # Separate records by PID availability
            has_pid = df[df[pid_field].notna()].copy()
            no_pid = df[df[pid_field].isna()].copy()

        else:

            logger.error(
                f"PID field '{pid_field}' not found in DW table columns: "
                f"{df.columns.tolist()}"
            )
            continue

        logger.info(f"Total records: {len(df)}")
        logger.info(f"Records with PID: {len(has_pid)}")
        logger.info(
            f"Records without PID: {len(no_pid)} (will be skipped)"
        )

        unlocated_df = pd.DataFrame()

        # Generate PID features
        if has_pid.empty:

            logger.warning(
                "No records with PID to process. Skipping geolocation."
            )

        else:

            located_feature, unlocated_df = feature_locator.locate(
                records_df=has_pid,
                target_feature=target_sde_feature,
                pid_field=pid_field,
                sde_pid_field=DEFAULT_PID_FIELD,
                field_map_dict=TABLE_FIELD_MAPS.get(dw_table_name),
            )

            located_count = int(arcpy.GetCount_management(located_feature)[0])
            logger.info(f"Total located features: {located_count}")

            # Load into SDE
            if located_count > 0:
                load_to_sde(
                    located_feature,
                    target_sde_feature,
                    truncate=truncate_and_load,
                )
                processed_features.append(target_feature_name)

            else:
                logger.warning("No located features to load.")

        # Write per-table failure report when any records could not be located
        if not no_pid.empty or not unlocated_df.empty:
            report_path = os.path.join(
                reports_dir, f"{feature_label}_failed_locates.xlsx"
            )
            generate_report(no_pid, unlocated_df, report_path)

    return processed_features


if __name__ == "__main__":

    start_time = time.time()
    logger.info(f"Start: {time.asctime()}")
    logger.info(SEPARATOR)

    # Create scratch workspace
    logger.info("Creating local scratch workspace...")
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    scratch_ws = create_fgdb(SCRATCH_DIR, "Scratch.gdb")

    # Run main processing
    os.makedirs(REPORTS_DIR, exist_ok=True)
    processed_features = main(
        scratch_workspace=scratch_ws,
        output_rw_sde=SDEADM_RW,
        dw_stg=DW_STG,
        dw_source_tables=DW_SOURCE_TABLES,
        pid_field_map=PID_FIELD_MAP,
        spatial_reference=SPATIAL_REFERENCE,
        exports_dir=EXPORTS_DIR,
        reports_dir=REPORTS_DIR,
        truncate_and_load=TRUNCATE_AND_LOAD,
        geometry_mode=GEOMETRY_MODE,
    )

    # Replicate to RO SDE
    if processed_features:
        replicate_to_ro(SDEADM_RW, SDEADM_RO, processed_features)

    # Summary
    elapsed = time.time() - start_time
    logger.info(SEPARATOR)
    logger.info(f"End: {time.asctime()}")
    logger.info(f"Elapsed: {elapsed:.1f} seconds")
