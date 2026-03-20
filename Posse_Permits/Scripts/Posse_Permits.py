"""
~25 minute run-time
"""

import sys
import os
import datetime
import time
import traceback
import logging
import arcpy

from configparser import ConfigParser

from HRMutils import (
    setupLog,
    send_mail,
    create_fgdb,
    remove_duplicates_from_csv
)

from Posse_Permits_Processing import (
    civic_join,
    retire_civ_join,
    geocode_addresses,
    Geocoder,
    generate_pid_points,
    parse_duplicates,
    create_relationship,
    failed_locating_report,
    add_unique_occupancy_types_to_related_table,
    trunc_load_ro,
    remove_duplicates_from_csv
)

# Directories
WORKING_DIR = os.path.dirname(sys.path[0])
WD_FOLDER_NAME = os.path.basename(WORKING_DIR)

FILE_NAME = os.path.basename(__file__)
FILE_NAME_BASE = os.path.splitext(FILE_NAME)[0]

SCRIPTS_DIR = os.path.join(WORKING_DIR, "Scripts")
SCRATCH_DIR = os.path.join(WORKING_DIR, "Scratch")
REPORTS_DIR = os.path.join(SCRIPTS_DIR, "Reports")
EXPORTS_DIR = os.path.join(WORKING_DIR, "Scripts", "Exports")

# Config Parser
config = ConfigParser()
config.read("E:\\HRM\\Scripts\\Python\\config.ini")

# Logging
log_file = os.path.join(config.get('LOGGING', 'logDir'), WD_FOLDER_NAME, f"{str(datetime.date.today())}_{FILE_NAME_BASE}.log")
logger = setupLog(log_file)
log_server = config.get('LOGGING', 'serverName')

console_handler = logging.StreamHandler()
log_formatter = logging.Formatter('%(asctime)s | %(levelname)s | FUNCTION: %(funcName)s | Msgs: %(message)s', datefmt='%d-%b-%y %H:%M:%S')
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)  # print logs to console

# Environment variables
arcpy.SetLogHistory(False)
arcpy.env.overwriteOutput = True

# Local variables
SDEADM_RW = config.get('SDEADM_RW', 'sdeFile')
SDEADM_RO = config.get('SDEADM_RO', 'sdeFile')
DW_STG = config.get('HRM_STG', 'sdeFile')
DW_STG = r"E:\HRM\Scripts\SDE\STG_DW_ArcgisServer_PROD.sde"

# Data Warehouse Processing Tables - the source tables for permit data
DW_PROCESSING_TABLES = [
    (os.path.join(DW_STG, 'OPENDATA_SOURCE.PPLC_ISSUED_BUILDING_PERMIT'), "Civic_ID", "Building"),
    (os.path.join(DW_STG, "OPENDATA_SOURCE.PPLC_ISSUED_CONSTRUCTION_PERMIT"), "Civic_ID", "Construction"),
    (os.path.join(DW_STG, "OPENDATA_SOURCE.PPLC_ISSUED_ENGINEERING_PERMIT"), "HRM_ID", "Engineering"),
    (os.path.join(DW_STG, "OPENDATA_SOURCE.PPLC_ISSUED_HALIFAX_WATER_PERMIT"), "Civic_ID", "Halifax_Water"),
    (os.path.join(DW_STG, "OPENDATA_SOURCE.PPLC_ISSUED_LAND_USE_APPROVAL_PERMIT"), "Civic_ID", "Land_Use_Approval"),
    (os.path.join(DW_STG, "OPENDATA_SOURCE.PPLC_ISSUED_PUBLIC_WORKS_ROW_PERMIT"), "HRM_ID", "Public_Works_ROW"),
]

LND_CIVIC_ADDRESS = os.path.join(SDEADM_RW, "SDEADM.LND_civic_address")
RELATED_TABLE_NAME = "LND_PPLC_Permit_Info"
FEATURE_DATASET_NAME = "LND_PPLC_Permits"

SCHEMAS_GDB = os.path.join(SCRATCH_DIR, "Schemas.gdb")
RELATED_PERMITS_TEMPLATE = os.path.join(SCHEMAS_GDB, "related_permits")
LOCATED_FEATURES_TEMPLATE = os.path.join(SCHEMAS_GDB, "located_permits")

FAILS_REPORT = os.path.join(REPORTS_DIR, "PPLCGeocoding_fails.xlsx")

LOAD_FEATURES_ONLY = True


def main(scratch_workspace: str, output_workspace: str, truncate_and_load=True):
    """
    Main processing function for PPLC Permits.
    Now reads directly from DW_PROCESSING_TABLES instead of CSV files.
    """

    logger.info(f"OUTPUT WORKSPACE: {output_workspace}")

    # Copy master related table to final output workspace
    logger.info(f"Creating related table, '{RELATED_TABLE_NAME}'...")
    master_related_table = arcpy.TableToTable_conversion(
        RELATED_PERMITS_TEMPLATE,
        scratch_workspace,
        RELATED_TABLE_NAME
    )[0]

    # Create feature dataset in final output workspace
    # Don't overwrite if just truncating and loading
    if truncate_and_load:
        feature_dataset = os.path.join(output_workspace, FEATURE_DATASET_NAME, )

    else:
        logger.info(f"Creating Feature Dataset, '{FEATURE_DATASET_NAME}'...")
        feature_dataset = arcpy.CreateFeatureDataset_management(
            out_dataset_path=output_workspace,
            out_name=FEATURE_DATASET_NAME,
            spatial_reference=arcpy.Describe(LND_CIVIC_ADDRESS).spatialReference
        )[0]

    # Create Geocoder
    # Export civics for geocoder
    local_civics = arcpy.FeatureClassToFeatureClass_conversion(
        LND_CIVIC_ADDRESS,
        os.path.join(SCRATCH_DIR, "Data.gdb"),
        "LND_civic_address",
    )[0]
    geocoder = Geocoder("geocoder", local_civics)

    # Process DataWarehouse tables directly (no longer using CSV exports)
    located_features = list()

    permit_features_count = len(DW_PROCESSING_TABLES)
    for permit_feature_count, feature_info in enumerate(DW_PROCESSING_TABLES, start=1):

        # VARIABLES - now unpacking table path, ID field, and permit type name
        source_table, feature_civ_id, permit_type = feature_info
        permit_feature_name = f"PPLC_{permit_type}_Permits"

        permit_duplicate_rows_csv = os.path.join(EXPORTS_DIR, f"{permit_feature_name}_duplicate_records.csv")
        permit_unique_rows_csv = os.path.join(EXPORTS_DIR, f"{permit_feature_name}_unique_records.csv")

        related_table_name = f"LND_{permit_feature_name}_rel_table"
        final_feature_name = f"LND_{permit_feature_name}"
        failed_locates_feature_name = f"LND_{permit_feature_name}_failed_locates"

        # PROCESSING
        logger.info(f"Processing permit {permit_feature_count}/{permit_features_count}) {permit_feature_name.upper()}...")

        # PREPARE SOURCE DATA FOR PROCESSING
        # Split table into unique permits and duplicates are based on Permit Number,
        #   prioritize where Active_Parcel == "Y"

        table_row_count = int(arcpy.management.GetCount(source_table)[0])

        if table_row_count == 0:

            logger.warning(f"WARNING: No data found for {source_table}")
            continue
            # TODO: Raise error

        unique_rows_df, only_duplicates_df = parse_duplicates(source_table, permit_feature_name, feature_civ_id)

        if "Occupancy_Type" in unique_rows_df.columns.tolist():
            only_duplicates_df = add_unique_occupancy_types_to_related_table(unique_rows_df, only_duplicates_df)

        # Delete Occupancy Type field
        permit_table_fields = [x for x in unique_rows_df.columns.tolist() if x.upper() not in ("OCCUPANCY_TYPE",)]
        unique_rows_df = unique_rows_df[permit_table_fields]

        # DataFrame --> CSV (intermediate step for arcpy compatibility)
        for dataframe, export_csv in (
                (unique_rows_df, permit_unique_rows_csv),
                (only_duplicates_df, permit_duplicate_rows_csv)
        ):
            dataframe.to_csv(export_csv, index=False)

        # CREATE TABLES TO HOLD RESULTS -> Final Feature, Related Table, Failed to Locate

        # RELATED TABLE
        # Create table from schema
        related_table = arcpy.CreateTable_management(
            out_path=scratch_workspace,
            out_name=related_table_name,
            template=master_related_table
        )[0]

        # Add any duplicate rows to the related table
        permit_duplicate_rows_csv, rows_removed = remove_duplicates_from_csv(permit_duplicate_rows_csv)
        logger.info(f"{rows_removed} rows removed from {permit_duplicate_rows_csv} (duplicates).")

        arcpy.Append_management(
            permit_duplicate_rows_csv,
            related_table,
            "NO_TEST",
        )
        logger.info(str(arcpy.GetMessages()))
        logger.info(f"Number of related table rows: {arcpy.GetCount_management(related_table)[0]}")

        # UNIQUE RECORDS TABLE
        # This table will be used as the input table in the geocoding processes
        final_permits_temp_table = arcpy.CreateTable_management(
            out_path=scratch_workspace,
            out_name=f"{final_feature_name}_unique",
            template=LOCATED_FEATURES_TEMPLATE
        )[0]

        # CHANGE Civic_ID TO HRM_ID field if source spreadsheet has HRM_ID
        if feature_civ_id == "HRM_ID":
            arcpy.AlterField_management(
                final_permits_temp_table,
                field="Civic_ID",
                new_field_name="HRM_ID",
                new_field_alias="HRM_ID"
            )

        arcpy.Append_management(
            permit_unique_rows_csv,
            final_permits_temp_table,
            "NO_TEST"
        )
        logger.info(str(arcpy.GetMessages()))

        logger.info(f"Number of unique table rows: {arcpy.GetCount_management(final_permits_temp_table)[0]}")

        df_permits = set(unique_rows_df['Permit_Number'].tolist())
        table_permits = [row[0] for row in arcpy.da.SearchCursor(final_permits_temp_table, "Permit_Number")]
        missing_permits = [x for x in df_permits if x not in table_permits]
        print(f"Missing permits: {len(missing_permits)}")

        # Use Update Cursor to add leading zeros to PIDs
        for tbl in related_table, final_permits_temp_table:

            with arcpy.da.UpdateCursor(tbl, "PID", "PID IS NOT NULL") as cursor:

                for row in cursor:
                    row[0] = row[0].zfill(8)
                    cursor.updateRow(row)

        # Append permits from related table to master related table
        arcpy.Append_management(
            inputs=related_table,
            target=master_related_table,
            schema_type="NO_TEST"
        )

        # Create final permits feature AFTER template feature has had PID field converted to text field
        final_permits_feature = arcpy.CreateFeatureclass_management(
            out_path=scratch_workspace,
            out_name=final_feature_name,
            geometry_type="POINT",
            spatial_reference=arcpy.Describe(LND_CIVIC_ADDRESS).spatialReference,
            template=LOCATED_FEATURES_TEMPLATE
        )[0]

        # Create table to hold failed locates with same schema as final table.
        failed_locates_feature = arcpy.CreateTable_management(
            out_path=scratch_workspace,
            out_name=failed_locates_feature_name,
            template=LOCATED_FEATURES_TEMPLATE
        )[0]

        for feature in final_permits_feature, failed_locates_feature:

            if feature_civ_id == "HRM_ID":
                arcpy.AlterField_management(
                    feature,
                    field="Civic_ID",
                    new_field_name="HRM_ID",
                    new_field_alias="HRM_ID"
                )

        # Create Tables that will need to be queried against for mapping
        permits_w_civid = arcpy.TableToTable_conversion(
            final_permits_temp_table,
            scratch_workspace,
            f"{permit_feature_name}_with_civid",
            where_clause=f"{feature_civ_id} IS NOT NULL"
        )[0]

        civ_info_no_id_records = arcpy.TableToTable_conversion(
            final_permits_temp_table,
            scratch_workspace,
            f"{permit_feature_name}_civ_info",
            where_clause=f"{feature_civ_id} IS NULL AND Civic_Number IS NOT NULL AND Street_Name IS NOT NULL AND Community IS NOT NULL"
        )[0]

        no_civ_info_records = arcpy.TableToTable_conversion(
            final_permits_temp_table,
            scratch_workspace,
            f"{permit_feature_name}_NO_civ_info",
            where_clause=f"{feature_civ_id} IS NULL AND Civic_Number IS NULL AND Street_Name IS NULL AND Community IS NULL"
        )[0]

        pid_only_records = arcpy.TableToTable_conversion(
            final_permits_temp_table,
            scratch_workspace,
            f"{permit_feature_name}_only_pid",
            where_clause=f"{feature_civ_id} IS NULL AND Civic_Number IS NULL AND Street_Name IS NULL AND Community IS NULL AND PID IS NOT NULL"
        )[0]
        num_pid_only_records = arcpy.GetCount_management(pid_only_records)[0]

        no_info_records = arcpy.TableToTable_conversion(
            final_permits_temp_table,
            scratch_workspace,
            f"{permit_feature_name}_null_fields",
            where_clause=f"{feature_civ_id} IS NULL AND Civic_Number IS NULL AND Street_Name IS NULL AND Community IS NULL AND PID IS NULL"
        )[0]

        geocode_features = [
            civ_info_no_id_records,  # retired data with Civic_Number or Street_Name or Community
            # Failed CIV_ID joins?
        ]

        # LOCATE FEATURES USING CIVIC ID

        # If Civic_ID IS NOT NULL
        input_row_count = int(arcpy.GetCount_management(permits_w_civid)[0])
        if input_row_count == 0:
            logger.debug(f"'{permits_w_civid}' feature has no rows! Skipping Civic Join.")

        else:
            # Join to LND_civic_address
            civic_unjoined_records, civic_joined_records = civic_join(
                permits=permits_w_civid,
                output_workspace=scratch_workspace,
                permits_join_field=feature_civ_id
            )

            # Append joined records to final feature
            arcpy.Append_management(
                inputs=civic_joined_records,
                target=final_permits_feature,
                schema_type="NO_TEST",
            )

            # Join to CIVIC RETIRE FEATURE
            retired_unjoined_records, retired_joined_records = retire_civ_join(
                civic_unjoined_records,
                scratch_workspace,
                feature_civ_id
            )

            # # Add failed joins to list of features to geocode
            # geocode_features.append(retired_unjoined_records)

            # Append joined records to final feature
            arcpy.Append_management(
                inputs=retired_joined_records,
                target=final_permits_feature,
                schema_type="NO_TEST"
            )

            arcpy.Append_management(
                inputs=retired_unjoined_records,
                target=failed_locates_feature,
                schema_type="NO_TEST"
            )

        # LOCATE FEATURES USING GEOCODER
        # Geocode features: features with civic address info - failed civic joins, features with no civic ID
        logger.info(f"Geocoding features: {', '.join(geocode_features)}...")

        for geocode_feature in geocode_features:
            geocode_feature_name = arcpy.Describe(geocode_feature).name
            input_row_count = int(arcpy.GetCount_management(geocode_feature)[0])

            if input_row_count == 0:
                logger.info(f"'{geocode_feature_name}' feature has no rows! Skipping Geocoding.")

            else:

                geocoded_permits, ungeocoded_permits, ungeocodeable_permits = geocode_addresses(
                    permits=geocode_feature,
                    workspace=scratch_workspace,
                    output_feature_name=f"{geocode_feature_name}_geocodes",
                    locator=geocoder
                )

                # Append geocoded records to final feature
                arcpy.Append_management(
                    inputs=geocoded_permits,
                    target=final_permits_feature,
                    schema_type="NO_TEST"
                )

                # if non-geocoded data have a PID. generate point feature at centre of parcel, otherwise add to fails
                for permits in ungeocoded_permits, ungeocodeable_permits:

                    # Check that count of ungeocoded_permits, ungeocodeable_permits is not None before continuing
                    input_row_count = int(arcpy.GetCount_management(permits)[0])
                    if input_row_count == 0:
                        logger.debug(
                            f"'{os.path.basename(permits)}' feature has no rows! Skipping generating PID point locations.")

                    else:

                        pid_point_features, unlocated_features, no_pid_features = generate_pid_points(
                            permits=permits,
                            output_workspace=scratch_workspace,
                            reference_feature=final_permits_temp_table
                        )
                        logger.info(f"Number of located PID records: {arcpy.GetCount_management(pid_point_features)[0]}")
                        logger.info(f"Number of unlocated PID records: {arcpy.GetCount_management(unlocated_features)[0]}")

                        # Append failed pid points - FIRST TIME WE'RE APPENDING TO FAILED LOCATES TABLE
                        logger.info(f"Appending to FAILED LOCATES feature ('{failed_locates_feature}')")
                        for feature in unlocated_features, no_pid_features:
                            logger.info(f"{feature} - Count: {arcpy.GetCount_management(feature)}")
                            arcpy.Append_management(
                                inputs=feature,
                                target=failed_locates_feature,
                                schema_type="NO_TEST"
                            )

                        # Append PIC located records to final feature
                        arcpy.Append_management(
                            inputs=pid_point_features,
                            target=final_permits_feature,
                            schema_type="NO_TEST"
                        )

        # LOCATE FEATURES USING PID
        # Check that count of pid_only_records is not None before continuing
        input_row_count = int(arcpy.GetCount_management(pid_only_records)[0])
        if input_row_count == 0:
            logger.debug(f"'{pid_only_records}' feature has no rows! Skipping generating PID point locations.")

        else:
            pid_point_features, unlocated_features, no_pid_features = generate_pid_points(
                permits=pid_only_records,
                output_workspace=scratch_workspace,
                reference_feature=final_permits_temp_table
            )

            logger.info(f"Number of located PID records: {arcpy.GetCount_management(pid_point_features)[0]}")
            logger.info(f"Number of unlocated PID records: {arcpy.GetCount_management(unlocated_features)[0]}")

            # Append located records to final feature
            arcpy.Append_management(
                inputs=pid_point_features,
                target=final_permits_feature,
                schema_type="NO_TEST"
            )

            # Append failed pid points
            for feature in unlocated_features, no_pid_features:
                logger.info(f"\t{feature} - Count: {arcpy.GetCount_management(feature)}")
                arcpy.Append_management(
                    inputs=feature,
                    target=failed_locates_feature,
                    schema_type="NO_TEST"
                )

        # MERGE UNSUCCESSFUL LOCATES
        logger.info(f"{no_info_records} - Count: {arcpy.GetCount_management(no_info_records)}")
        arcpy.Append_management(
            inputs=no_info_records,
            target=failed_locates_feature,
            schema_type="NO_TEST"
        )

        num_located = arcpy.GetCount_management(final_permits_feature)[0]
        logger.info(f"Total located features: {num_located}")

        # Total number of unlocated features
        num_unlocated = arcpy.GetCount_management(failed_locates_feature)[0]
        logger.info(f"Total unlocated features: {num_unlocated}")

        #######################################################################################
        #    ----------------------------  PROCESS FINISHED  ----------------------------     #
        #######################################################################################

        # TODO: Delete FORMATTED ADDRESS field
        arcpy.DeleteField_management(
            final_permits_feature,
            ["FORMATTEDADDRESS",]
        )

        feature_name_lookup = {
            "LND_PPLC_Building_Permits": 'LND_PPLC_Building_Permits',
            "LND_PPLC_Construction_Permits": 'LND_PPLC_Construction_Permits',
            "LND_PPLC_Engineering_Permits": 'LND_PPLC_Engineering_Permits',
            "LND_PPLC_Halifax_Water_Permits": 'LND_PPLC_HW_Permits',
            'LND_PPLC_Land_Use_Approval_Permits': 'LND_PPLC_LU_Approval_Permits',
            'LND_PPLC_Public_Works_ROW_Permits': 'LND_PPLC_PW_ROW_Permits',
        }

        feature_name = os.path.basename(final_permits_feature)
        feature_name = feature_name_lookup.get(feature_name, feature_name)
        out_feature = os.path.join(feature_dataset, feature_name)

        located_features.append(out_feature)

        if not truncate_and_load:
            # Copy final features to output workspace

            # Feature Dataset features
            logger.info(f"Exporting feature '{out_feature}'...")
            arcpy.Copy_management(
                in_data=final_permits_feature,
                out_data=out_feature
            )

            # Add Global IDs
            logger.info("Adding GlobalIDs...")
            arcpy.AddGlobalIDs_management(out_feature)

            # Grant view privileges
            logger.info("Granting SELECT privileges for PUBLIC db role...")
            arcpy.ChangePrivileges_management(
                out_feature,
                user="PUBLIC",
                View="GRANT"
            )

            # For Engineering, Construction, Halifax Water, the following fields should not exist:
            # Estimated_Project_Value
            # Existing_Residential_Units
            # Total_End_Residential_Units
            # Building_Footprint_Area
            # Number_of_Storeys

            if feature_name in (
                    # 'LND_PPLC_Building_Permits',
                    # 'LND_PPLC_Construction_Permits',  #
                    'LND_PPLC_Engineering_Permits',  #
                    'LND_PPLC_HW_Permits',  #
                    'LND_PPLC_LU_Approval_Permits',  #
                    'LND_PPLC_PW_ROW_Permits',  #
            ):

                delete_fields = [
                    'Estimated_Project_Value',
                    'Existing_Residential_Units',
                    'Total_End_Residential_Units',
                    'Building_Footprint_Area',
                    'Number_of_Storeys',
                ]
                arcpy.DeleteField_management(
                    out_feature,
                    delete_fields
                )

            if feature_name in ('LND_PPLC_Construction_Permits', ):
                # Don't delete 'Estimated_Project_Value' from Construction permits
                delete_fields = [
                    'Existing_Residential_Units',
                    'Total_End_Residential_Units',
                    'Building_Footprint_Area',
                    'Number_of_Storeys',
                ]
                arcpy.DeleteField_management(
                    out_feature,
                    delete_fields
                )

            # For Land Use Approval and PW ROW the following field should not exist, additional to the ones above:
            # Date_Inspection_Completed
            # If land use approvals, delete MOST_RECENT_INSPECTION field
            if feature_name in (
                    'LND_PPLC_LU_Approval_Permits',
                    'LND_PPLC_PW_ROW_Permits',
            ):
                delete_fields = ['Date_Inspection_Completed', 'Most_Recent_Inspection']
                arcpy.DeleteField_management(
                    out_feature,
                    delete_fields
                )

        # TRUNCATE & LOAD
        else:
            sde_feature = os.path.join(output_workspace, FEATURE_DATASET_NAME, feature_name)

            logger.info(f"Truncating {sde_feature}")
            arcpy.DeleteRows_management(sde_feature)

            logger.info(f"Loading {sde_feature}")
            with arcpy.EnvManager(preserveGlobalIds=False):
                arcpy.Append_management(
                    final_permits_feature,
                    sde_feature,
                    "NO_TEST"
                )
            logger.info(str(arcpy.GetMessages()))

    logger.info(f"Exporting related table '{RELATED_TABLE_NAME}'...")
    final_related_table = arcpy.Copy_management(
        in_data=master_related_table,
        out_data=os.path.join(output_workspace, RELATED_TABLE_NAME)
    )[0]

    # Add Global IDs
    logger.info("Adding GlobalIDs...")
    arcpy.AddGlobalIDs_management(final_related_table)

    # Grant view privileges
    logger.info("Granting SELECT privileges for PUBLIC db role...")
    arcpy.ChangePrivileges_management(
        final_related_table,
        user="PUBLIC",
        View="GRANT"
    )

    for feature in located_features:
        create_relationship(
            origin_table=feature,
            related_table=final_related_table
        )


if __name__ == "__main__":

    start_time = time.asctime(time.localtime(time.time()))
    logger.info(f"Start: {start_time}")
    logger.info("-----------------------")

    # try:

    # Use local utility to recreate scratch file geodatabase.
    logger.info("Creating local scratch workspace...")
    local_workspace = create_fgdb(SCRATCH_DIR, "Scratch.gdb")

    # Updated call - no longer passing permit_files_dir since we read from DW tables directly
    main(local_workspace, SDEADM_RW, LOAD_FEATURES_ONLY)

    trunc_load_ro(SDEADM_RW, SDEADM_RO)

    logger.info("Getting report for failed locates...")

    failed_locating_report(local_workspace, FAILS_REPORT)

    # except:
    #     # Return any python specific errors as well as any errors from the geoprocessor
    #     tb = sys.exc_info()[2]
    #     tbinfo = traceback.format_tb(tb)[0]
    #     pymsg = "PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + \
    #             str(sys.exc_info()[0]) + ": " + str(sys.exc_info()[1]) + "\n"
    #     logger.error(pymsg)
    #
    #     # Send e-mail in case of error
    #     send_mail(
    #         to=str(config.get('EMAIL', 'recipients')).split(','),
    #         subject='ERROR - POSSE Permits Failed',
    #         text=f'{log_server} / {FILE_NAME} \n \n {str(pymsg)}'
    #     )
    #
    #     # Exit program
    #     sys.exit()

    # Close the Log File:
    end_time = time.asctime(time.localtime(time.time()))
    logger.info("-----------------------")
    logger.info(f"End: {end_time}")

    # TODO: Create report of failed locates with failed_locates_feature

    # TODO: Add logging - may want to add hrm logging util to posse_permits.py to capture this info or
    #  add additional log/print statements in lrs_dyn_seg_view.py from posse_permits.py