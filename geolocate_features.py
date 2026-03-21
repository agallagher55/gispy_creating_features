"""
Geolocate planning and subdivision application features by PID (Parcel ID).

Reads application data from Data Warehouse staging tables, locates each record
at the centroid of its associated parcel polygon, and loads the results into
existing SDE feature classes via truncate-and-load.

Modeled after: Posse_Permits/Scripts/Posse_Permits.py
"""

import os
import ast
import time
import datetime

import arcpy
import pandas as pd

from configparser import ConfigParser

from gispy.utils import create_fgdb, setupLog

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

FAILS_REPORT = os.path.join(
    REPORTS_DIR, f"{FILE_NAME_BASE}_failed_locates.xlsx"
)

SEPARATOR = '=' * 60

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------


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


def extract_primary_pid(pid_value):
    """
    Extract and zero-pad the first PID from a potentially
    comma-separated list of PIDs.

    :param pid_value: A single PID string or comma-separated PID list
    :return: Zero-padded 8-character PID string, or None if empty/null
    """
    if not pid_value or pd.isna(pid_value):
        return None
    first = str(pid_value).split(",")[0].strip()
    return first.zfill(8) if first else None


def get_parcel_geometry(parcel_fc, pid):
    """
    Get the centroid point of a parcel polygon identified by PID.
    Uses trueCentroid if it falls within the parcel, otherwise uses
    labelPoint.
    Adapted from Posse_Permits/Scripts/Posse_Permits_Processing.py

    :param parcel_fc: Path to the parcel polygon feature class
    :param pid: The PID string (8-char, zero-padded)
    :return: (X, Y) tuple or None if PID not found
    """
    with arcpy.da.SearchCursor(
        parcel_fc, ["SHAPE@", "PID"], f"PID LIKE '{pid}'"
    ) as cursor:
        for row in cursor:
            parcel_geometry = row[0]

            center_point = parcel_geometry.trueCentroid

            if parcel_geometry.contains(center_point):
                return center_point.X, center_point.Y

            else:
                label_point = parcel_geometry.labelPoint
                return label_point.X, label_point.Y

    return None


def generate_pid_points(
    records_df, scratch_workspace, target_feature, parcel_fc,
    pid_field, sde_pid_field, spatial_reference, exports_dir,
    primary_pid_col=None
):

    """
    Generate point features from PID-based parcel centroids.
    Adapted from Posse_Permits/Scripts/Posse_Permits_Processing.py

    :param records_df: DataFrame of records WITH valid PIDs
    :param scratch_workspace: Path to scratch file geodatabase
    :param target_feature: Path to the target SDE feature (used as schema
        template)
    :param parcel_fc: Path to LND_parcel_polygon feature class
    :param pid_field: Name of the PID field in the source data
    :param sde_pid_field: Name of the PID field in the SDE feature class
    :param spatial_reference: ArcPy SpatialReference object for output
        features
    :param exports_dir: Path to directory for CSV exports
    :param primary_pid_col: Optional column name containing pre-extracted
        single PIDs for geolocation. Use when pid_field may hold
        comma-separated multi-value PID strings. Defaults to pid_field.
    :return: (located_feature_path, unlocated_df) - located point feature
        class and DataFrame of unlocated records
    """
    feature_name = os.path.basename(target_feature).replace("SDEADM.", "")
    temp_feature_name = f"{feature_name}_pid_located"

    logger.info(
        f"Generating PID points for '{feature_name}' "
        f"({len(records_df)} records)..."
    )

    # Export DataFrame to CSV for arcpy compatibility
    os.makedirs(exports_dir, exist_ok=True)
    export_csv = os.path.join(exports_dir, f"{feature_name}_pid_records.csv")
    records_df.to_csv(export_csv, index=False)

    # Create point feature class in scratch workspace using target as template
    temp_feature = arcpy.CreateFeatureclass_management(
        out_path=scratch_workspace,
        out_name=temp_feature_name,
        geometry_type="POINT",
        template=target_feature,
        spatial_reference=spatial_reference,
    )[0]

    # Append CSV records into the temp feature
    arcpy.Append_management(
        inputs=export_csv,
        target=temp_feature,
        schema_type="NO_TEST",
    )
    logger.info(
        f"\tAppended {arcpy.GetCount_management(temp_feature)[0]}"
        " records to temp feature"
    )

    # Determine which column holds the single PID to use for geolocation.
    # primary_pid_col is pre-extracted when pid_field may hold multi-value
    # comma-separated strings (e.g. "00130278, 00130286").
    lookup_col = primary_pid_col if primary_pid_col else pid_field

    # Build centroid lookup: {primary_pid: (x, y)}
    pids = records_df[lookup_col].dropna().unique().tolist()
    logger.info(f"\tLooking up centroids for {len(pids)} unique PIDs...")

    centroids = {}
    for pid in pids:
        geometry = get_parcel_geometry(parcel_fc, pid)
        centroids[pid] = geometry

    found_count = sum(1 for v in centroids.values() if v is not None)
    logger.info(f"\tFound parcel geometry for {found_count}/{len(pids)} PIDs")

    # Update feature geometry with centroid coordinates.
    # The SDE field (sde_pid_field) may hold a multi-value string, so
    # extract_primary_pid is applied to get the lookup key.
    unfound_pids = []

    with arcpy.da.UpdateCursor(
        temp_feature, [sde_pid_field, "SHAPE@XY"]
    ) as cursor:

        for row in cursor:

            row_pid = extract_primary_pid(row[0])
            centroid = centroids.get(row_pid)

            if centroid:

                row[1] = centroid
                cursor.updateRow(row)

            else:
                unfound_pids.append(row_pid)
                cursor.deleteRow()

    del cursor

    logger.info(
        f"\tLocated: {arcpy.GetCount_management(temp_feature)[0]} features"
    )
    logger.info(
        f"\tUnlocated PIDs (not found in parcel layer): {len(unfound_pids)}"
    )

    # Build DataFrame of unlocated records
    unlocated_df = records_df[records_df[lookup_col].isin(unfound_pids)]

    return temp_feature, unlocated_df


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
    pid_field_map, spatial_reference, exports_dir, truncate_and_load=True
):
    """
    Main processing function. Loops over configured DW source tables,
    geolocates records by PID, and loads into SDE.

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
    :param truncate_and_load: If True, truncate target features before loading
    """
    logger.info(f"OUTPUT WORKSPACE: {output_rw_sde}")
    logger.info(f"DW STAGING: {dw_stg}")

    all_no_pid = []
    all_unlocated = []
    processed_features = []

    parcel_fc = os.path.join(
        output_rw_sde, "SDEADM.LND_parcels", "SDEADM.LND_parcel_polygon"
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

        # Clean PID field and extract primary PIDs for geolocation.
        # The PIDs field may hold a comma-separated list (e.g.
        # "00130278, 00130286"); extract_primary_pid takes the first value.
        # The original multi-value string is preserved in pid_field for
        # storage in the SDE attribute.
        if pid_field in df.columns:

            df[pid_field] = df[pid_field].astype(str).replace(
                {"nan": None, "None": None}
            )

            # Separate records by PID availability
            has_pid = df[df[pid_field].notna()].copy()
            no_pid = df[df[pid_field].isna()].copy()

            # Extract and zero-pad the first PID from each (potentially
            # multi-value) PIDs string into a dedicated geolocation column.
            has_pid["_primary_pid"] = has_pid[pid_field].apply(
                extract_primary_pid
            )

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

        # Track no-PID records for reporting
        if not no_pid.empty:

            no_pid_report = no_pid.copy()
            no_pid_report["source_table"] = dw_table_name
            all_no_pid.append(no_pid_report)

        # Generate PID points
        if has_pid.empty:

            logger.warning(
                "No records with PID to process. Skipping geolocation."
            )
            continue

        located_feature, unlocated_df = generate_pid_points(
            records_df=has_pid,
            scratch_workspace=scratch_workspace,
            target_feature=target_sde_feature,
            parcel_fc=parcel_fc,
            pid_field=pid_field,
            sde_pid_field=DEFAULT_PID_FIELD,
            spatial_reference=spatial_reference,
            exports_dir=exports_dir,
            primary_pid_col="_primary_pid",
        )

        # Track unlocated records for reporting
        if not unlocated_df.empty:

            unlocated_report = unlocated_df.copy()
            unlocated_report["source_table"] = dw_table_name
            all_unlocated.append(unlocated_report)

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

    return processed_features, all_no_pid, all_unlocated


if __name__ == "__main__":

    start_time = time.time()
    logger.info(f"Start: {time.asctime()}")
    logger.info(SEPARATOR)

    # Create scratch workspace
    logger.info("Creating local scratch workspace...")
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    scratch_ws = create_fgdb(SCRATCH_DIR, "Scratch.gdb")

    # Run main processing
    processed_features, all_no_pid, all_unlocated = main(
        scratch_workspace=scratch_ws,
        output_rw_sde=SDEADM_RW,
        dw_stg=DW_STG,
        dw_source_tables=DW_SOURCE_TABLES,
        pid_field_map=PID_FIELD_MAP,
        spatial_reference=SPATIAL_REFERENCE,
        exports_dir=EXPORTS_DIR,
        truncate_and_load=TRUNCATE_AND_LOAD,
    )

    # Replicate to RO SDE
    if processed_features:
        replicate_to_ro(SDEADM_RW, SDEADM_RO, processed_features)

    # Generate failure report
    no_pid_combined = (
        pd.concat(all_no_pid, ignore_index=True)
        if all_no_pid else pd.DataFrame()
    )
    unlocated_combined = (
        pd.concat(all_unlocated, ignore_index=True)
        if all_unlocated else pd.DataFrame()
    )

    if not no_pid_combined.empty or not unlocated_combined.empty:
        os.makedirs(REPORTS_DIR, exist_ok=True)
        generate_report(no_pid_combined, unlocated_combined, FAILS_REPORT)

    # Summary
    elapsed = time.time() - start_time
    logger.info(SEPARATOR)
    logger.info(f"End: {time.asctime()}")
    logger.info(f"Elapsed: {elapsed:.1f} seconds")
