import sys
import os
import arcpy
import re
import pandas as pd
import numpy as np
from configparser import ConfigParser

# Directories
WORKING_DIR = os.path.dirname(sys.path[0])
SCRATCH_DIR = os.path.join(WORKING_DIR, "Scratch")

# Config Parser
config = ConfigParser()
config.read("E:\\HRM\\Scripts\\Python\\config.ini")

# Environment variables
arcpy.env.preserveGlobalIds = True
arcpy.SetLogHistory(False)
arcpy.env.overwriteOutput = True

# Local variables
SDEADM_RW = config.get('SDEADM_RW', 'sdeFile')

LND_PARCEL_POLYGON = os.path.join(SDEADM_RW, 'SDEADM.LND_parcels', "SDEADM.LND_parcel_polygon")

LND_CIVIC_ADDRESS = os.path.join(SDEADM_RW, "SDEADM.LND_civic_address")
RETIRED_CIVICS = os.path.join(SDEADM_RW, "SDEADM.LND_retire_civ")
TRN_STREET = os.path.join(SDEADM_RW, "SDEADM.TRN_streets_routes", "SDEADM.TRN_street")

PERMIT_INFO_FEATURE = "SDEADM.LND_PPLC_Permit_Info"

LND_PPLC_FEATURES = [
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_Building_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_Construction_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_Engineering_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_HW_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_LU_Approval_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_PW_ROW_Permits",
]


class Geocoder:

    field_map = ("'PointAddress.HOUSE_NUMBER LND_civic_address.FULL_CIVIC';"
                 "'PointAddress.STREET_NAME LND_civic_address.STR_NAME';"
                 "'PointAddress.DISTRICT LND_civic_address.DISTRICT';"
                 "'PointAddress.STREET_SUFFIX_TYPE LND_civic_address.STR_TYPE';"
                 "'PointAddress.SUB_ADDRESS_UNIT LND_civic_address.UNIT_NUM';"
                 "'PointAddress.CITY LND_civic_address.GSA_NAME';"
                 "'PointAddress.SUBREGION LND_civic_address.MUN_CODE';"
                 "'PointAddress.POSTAL LND_civic_address.CIV_POSTAL'")

    def __init__(self, name, reference_data):
        self.name = name
        self.reference_data = f"{reference_data} PointAddress"
        self.locator = self.get_locator()

    def get_locator(self):

        """
        The get_locator function creates a locator file from the reference data.

        :param self: Reference the object itself
        :return: The output_locator variable
        """

        output_locator = os.path.join(SCRATCH_DIR, f'{self.name}.loc')

        if arcpy.Exists(output_locator):
            print(f'Deleting geolocator {output_locator}')
            arcpy.Delete_management(output_locator)

        print("\nCreating locator...")

        print(f"Name: {self.name}")
        print(f"Reference Data: {self.reference_data}")
        print(f"Field Mapping: {Geocoder.field_map}")

        arcpy.geocoding.CreateLocator(
            country_code="CAN",
            primary_reference_data=self.reference_data,
            field_mapping=Geocoder.field_map,
            out_locator=output_locator,
            language_code="ENG",
            alternatename_tables=None,
            alternate_field_mapping=None,
            custom_output_fields=None,
            # precision_type="GLOBAL_HIGH"
            precision_type="LOCAL_EXTRA_HIGH"
        )

        return output_locator

    def geocode_addresses(self, table, output_workspace, output_feature_name: str, score_minimum: int = 60,
                          address_field: str = "FORMATTEDADDRESS"):
        """
        The geocode_addresses function takes a table, an output workspace, and an output feature name.
        It also takes two optional parameters: score_minimum (defaults to 60) and address_field (defaults to &quot;FORMATTEDADDRESS&quot;).
        The function returns a tuple of geocoded addresses that meet the score minimum as well as ungeocoded addresses that do not meet the score minimum.


        :param self: Reference the class itself
        :param table: Specify the input table that contains the addresses to be geocoded
        :param output_workspace: Specify the location of where the geocoded and ungeocoded features will be saved
        :param output_feature_name: str: Name the output feature class
        :param score_minimum: int: Set the minimum score that a geocoded address must have to be included in the output feature class
        :param address_field: str: Specify the field in the table that contains addresses to be geocoded
        :return: The geocoded and ungeocoded features
        """

        table_view = arcpy.MakeTableView_management(table, "geocoding layer")[0]

        geocoded_rows = os.path.join(output_workspace, "geocoded_rows")

        arcpy.GeocodeAddresses_geocoding(
            table_view,  # Table View
            self.locator,
            f"'Single Line Input' {address_field} VISIBLE NONE",
            geocoded_rows,
            "STATIC",
            None,
            "ADDRESS_LOCATION",
            "Address",
            "MINIMAL"
        )

        geocoded = arcpy.Select_analysis(
            geocoded_rows,
            os.path.join(output_workspace, output_feature_name),
            f"Score > {score_minimum}"
        ).getOutput(0)

        below_threshold_features = arcpy.Select_analysis(
            geocoded_rows,
            os.path.join(output_workspace, f"{output_feature_name}_below_threshold"),
            f"Score < {score_minimum}"
        ).getOutput(0)

        geocoded_drop_fields = ["Score", "Match_type", "Match_addr", "Addr_type", "Status"]

        for feature in geocoded, below_threshold_features:
            arcpy.DeleteField_management(
                in_table=feature,
                drop_field=geocoded_drop_fields
            )

        return geocoded, below_threshold_features


def add_unique_occupancy_types_to_related_table(unique_df, duplicates_df):

    print(f"\nAdding unique rows with an Occupancy Type to duplicates table...")

    permit_category = duplicates_df.iloc[0]["Permit_Category"]

    # Get fields of duplicates df - Duplicates df has extra field: "Permit_Category"
    duplicates_fields = [x for x in duplicates_df.columns.tolist() if x in unique_df.columns.tolist()]

    # Remove rows from unique df where there is not Occupancy Type
    if "Occupancy_Type" in unique_df.columns.tolist():
        unique_df = unique_df[unique_df["Occupancy_Type"].notna()][duplicates_fields]

    # - For each unique record:
    #     - Check to see if the Permit_Number exists in the related table records

    duplicates_permit_numbers = duplicates_df['Permit_Number'].unique()
    unique_permits_not_in_duplicates = unique_df[~unique_df['Permit_Number'].isin(duplicates_permit_numbers)]

    unique_permits_in_duplicates = unique_df[unique_df['Permit_Number'].isin(duplicates_permit_numbers)]

    #         - If the Permit_Number does not exist in the related records
    #             - APPEND this 'unique record to the related records table
    # append_one = pd.concat([duplicates_df, unique_permits_not_in_duplicates], ignore_index=True)

    #         - If the Permit_Number exists in the related records
    #             - If the Occupancy_Type is found in the related records with this Permit_Number
    #                 - Do nothing

    #             - If the Occupancy_Type is not found in the related records with this Permit_Number
    #                 - APPEND this 'unique' record to the related records table

    # Perform a left anti-join to get rows from unique_permits_in_duplicates not present in duplicates_df
    append_two_df = unique_permits_in_duplicates.merge(
        duplicates_df,
        on=['Permit_Number', 'Occupancy_Type'],
        how='left',
        indicator=True)
    append_two_df = append_two_df[append_two_df['_merge'] == 'left_only'].drop(columns='_merge')

    for df in (unique_permits_not_in_duplicates, append_two_df):
        if not df.empty:
            # duplicates_df = duplicates_df.append(df)  # As of pandas 2.0, append (previously deprecated) was removed.
            duplicates_df = pd.concat([duplicates_df, df], ignore_index=True)

    duplicates_df.loc[:, "Permit_Category"] = permit_category

    dup_duplicates = duplicates_df[duplicates_df.duplicated(subset=['Occupancy_Type', 'Permit_Number'], keep=False)]

    return duplicates_df


def table_to_dataframe(input_table):
    """
    Convert an ArcGIS table (geodatabase table, feature class, or SDE table) to a pandas DataFrame.

    :param input_table: Path to the input table or feature class
    :return: pandas DataFrame containing the table data
    """
    print(f"\nConverting table '{input_table}' to DataFrame...")

    # Get field names (excluding geometry fields for feature classes)
    fields = [field.name for field in arcpy.ListFields(input_table)
              if field.type not in ('Geometry', 'Blob', 'Raster')]

    # Read data using SearchCursor
    row_count = int(arcpy.management.GetCount(input_table)[0])
    data = [row for row in arcpy.da.SearchCursor(input_table, fields)]

    # Create DataFrame
    df = pd.DataFrame(data, columns=fields)

    print(f"\tConverted {len(df)} rows with {len(fields)} fields")

    return df


def parse_duplicates(input_source, permits_feature, id_field):
    """
    Parse data into duplicates and non-duplicates. Prioritize where Active_Parcel == "Y"
    The get_duplicates function takes a table view as input and returns two dataframes:
        1. A dataframe containing all unique rows from the original table, with duplicates removed.
        2. A dataframe containing only duplicate rows from the original table, with duplicates removed.

    :param input_source: Path to the database table (DW_PROCESSING_TABLE) or CSV file
    :param permits_feature: Name of the permit feature (used for naming)
    :param id_field: The ID field to use (Civic_ID or HRM_ID)

    :return: A tuple of two dataframes
    """

    dtypes = {"PID": str, "Civic_Number": str}

    # Check if input is a CSV file or a database table
    if input_source.lower().endswith('.csv'):

        # Legacy CSV reading
        df = pd.read_csv(input_source, dtype=dtypes)

    else:
        # TODO: Make sure input source exists

        # Read from database table using arcpy

        df = table_to_dataframe(input_source)

        # Apply dtype conversions for consistency with CSV reading
        if 'PID' in df.columns:
            df['PID'] = df['PID'].astype(str).replace('nan', None).replace('None', None)

        if 'Civic_Number' in df.columns:
            df['Civic_Number'] = df['Civic_Number'].astype(str).replace('nan', None).replace('None', None)

    record_count = df.shape[0]
    print(f"Initial Dataframe row count: {record_count}")

    df = df.replace({np.nan: None})

    # Clean Work Description field - remove any extra spaces
    if 'Work_Description' in df.columns:
        df['Work_Description'] = df['Work_Description'].str.replace(r"\s{2,}", " ", regex=True)

        # Set max field length to 2000
        df['Work_Description'] = df['Work_Description'].str.slice(0, 2000)

    df[id_field] = pd.to_numeric(df[id_field], errors='coerce').astype('Int64')

    # Add FORMATTED ADDRESS field
    # Set none fields to '' for street name, community and civic number
    df[['Street_Name', 'Community']] = df[['Street_Name', 'Community']].fillna('')

    def address_formatter(row):
        if pd.isna(row["Civic_Number"]) or not row["Street_Name"] or not row["Community"]:
            return ''

        # Remove spaces from Civic_Number to account for full_civic
        civic_number_formatted = str(row["Civic_Number"]).replace(" ", "")

        return f"{civic_number_formatted} {row['Street_Name']}, {row['Community']} NOVA SCOTIA"

    def rearrange_cols(dataframe):
        rearranged_cols = dataframe.columns.tolist()

        if 'Date_Inspection_Completed' in rearranged_cols:
            index_most_recent_inspection = rearranged_cols.index('Most_Recent_Inspection')
            index_date_inspection_completed = rearranged_cols.index('Date_Inspection_Completed')

            if index_most_recent_inspection < index_date_inspection_completed:
                rearranged_cols.insert(index_most_recent_inspection + 1,
                                       rearranged_cols.pop(index_date_inspection_completed))
            else:
                rearranged_cols.insert(index_most_recent_inspection,
                                       rearranged_cols.pop(index_date_inspection_completed))

            return dataframe[rearranged_cols]

        return dataframe

    df["FORMATTEDADDRESS"] = df.apply(
        lambda row: address_formatter(row),
        axis=1
    )

    # Remove ObjectID fields
    df_fields = [x for x in df.columns.tolist() if "OBJECTID" not in x.upper()]
    df = df[df_fields]

    # Add Date_Inspection_Completed field next to Most_Recent_Inspection field
    df = rearrange_cols(df)

    df.sort_values(by=["Permit_Number", "Active_Parcel"], inplace=True)

    # Keep one Permit in main table, prioritize where Active parcel == "Y"
    duplicate_rows = df.duplicated(subset="Permit_Number", keep="last")

    # RELATED TABLE/DUPLICATE ROWS
    duplicates_df = df[duplicate_rows]

    # Identify duplicates where it's due to Occupancy Type - add all permit records to duplicates table, but
    #  also add one to unique_rows df to be mapped

    # Remove duplicates from the table of all duplicates based on specific rules and conditions
    if "Occupancy_Type" in df.columns.tolist():

        # Select duplicates based on "Permit_Number" column. Capture ALL duplicates.
        duplicates_df = df[df.duplicated(subset="Permit_Number", keep=False)]

        # Use boolean masks to identify which records will be removed from duplicates dataframe
        # Active parcels with unique PIDs, but only one occupancy type
        df_active_parcel_same_occ_type_unique_pids = duplicates_df.groupby('Permit_Number').apply(
            lambda x:
            x if (x['Active_Parcel'] == 'Y').all() and
                 (len(x['PID'].unique()) > 1 and len(x['Occupancy_Type'].unique()) == 1)
            else None
        ).reset_index(drop=True)

        # Apply rule 1: Select rows where all "Active_Parcel" values are 'Y',
        # number of unique "PID" values is greater than 1, and number of unique "Occupancy_Type" values is 1.
        rule1_remove_records_df = duplicates_df.groupby('Permit_Number').filter(
            lambda x:
            (x['Active_Parcel'] == 'Y').all() and
            (len(x['PID'].unique()) > 1 and len(x['Occupancy_Type'].unique()) == 1)
        ).groupby('Permit_Number').head(1)

        # identify which records will be removed from duplicates dataframe, rule 2
        # This needs to be modified
        rule2_a_df = duplicates_df.groupby('Permit_Number').filter(
            lambda x:
            len(x['PID'].unique()) > 1 and
            len(x['Occupancy_Type'].unique()) == 1
        )
        rule2_remove_records_df = rule2_a_df[rule2_a_df["Active_Parcel"] == "Y"]

        # Remove records from rule2_df that pull from the pool rule1 pulls from
        rule2_remove_records_df = rule2_remove_records_df.merge(df_active_parcel_same_occ_type_unique_pids,
                                                                indicator=True, how='left').query(
            '_merge == "left_only"').drop('_merge', axis=1)

        # Remove records that are IN rule1 or rule2 dataframes
        removed_df = duplicates_df.merge(rule1_remove_records_df, indicator=True, how='left').query(
            '_merge == "left_only"').drop('_merge', axis=1)

        final_df = removed_df.merge(rule2_remove_records_df, indicator=True, how='left').query(
            '_merge == "left_only"').drop('_merge', axis=1)
        duplicates_df = final_df

    # Delete all fields other than Permit_Number, PID, Occupancy Type, Active Parcel. This will be the related table.
    duplicate_table_fields = [
        x for x in ['Permit_Number', 'PID', 'Occupancy_Type', 'Active_Parcel'] if x in df.columns.tolist()
    ]
    duplicates_df = duplicates_df[duplicate_table_fields]

    # Add in one of the duplicate_permits_w_unique_occupancies to the unique rows dataframe to be mapped
    permit_table_fields = [x for x in df.columns.tolist() if x.upper() not in ("OCCUPANCY_TYPE",)]
    # unique_rows_df = df[~duplicate_rows][permit_table_fields]  # This needs at least one of every permit. Check BP-2020-00076
    unique_rows_df = df[~duplicate_rows]  # This needs at least one of every permit. Check BP-2020-00076

    # Add "Permit_Category" field to duplicates_df
    permit_category = permits_feature.replace("PPLC_", "").replace("_Permits", "")
    duplicates_df.loc[:, "Permit_Category"] = permit_category

    # Set Permit_Category as first field in the table
    column_order = ["Permit_Category"] + [col for col in duplicates_df.columns if col != "Permit_Category"]
    duplicates_df = duplicates_df.loc[:, column_order]

    return unique_rows_df, duplicates_df


def create_relationship(origin_table, related_table):

    origin_table_desc = arcpy.Describe(origin_table)
    origin_table_name = origin_table_desc.name

    origin_table_workspace = origin_table_desc.path

    relationship_lookup = {
        'LND_PPLC_Building_Permits': 'Building_has_permits',
        'LND_PPLC_Construction_Permits': 'Construction_has_permits',
        'LND_PPLC_Engineering_Permits': 'Engineering_has_permits',
        'LND_PPLC_HW_Permits': 'HW_has_permits',
        'LND_PPLC_LU_Approval_Permits': 'LU_Approval_has_permits',
        'LND_PPLC_PW_ROW_Permits': 'PW_ROW_has_permits',
    }

    # Relationship class CAN ONLY BE 30 CHARS IN LENGTH
    relationship_name = relationship_lookup.get(origin_table_name, origin_table_name)

    out_relationship_class = os.path.join(
        origin_table_workspace,
        relationship_name
    )

    print(f"\nCreating relationship class for {origin_table}...")
    print(f"\tRelationship class: {out_relationship_class}")

    if arcpy.Exists(out_relationship_class):
        arcpy.Delete_management(out_relationship_class)

    arcpy.management.CreateRelationshipClass(
        origin_table=origin_table,
        destination_table=related_table,
        out_relationship_class=out_relationship_class,
        relationship_type="SIMPLE",
        forward_label="Permits",
        backward_label="Mapped Permits",
        message_direction="NONE",
        cardinality="ONE_TO_MANY",
        attributed="NONE",
        origin_primary_key="Permit_Number",
        origin_foreign_key="Permit_Number",
    )

    return out_relationship_class


def civic_join(permits, output_workspace, permits_join_field: str = "Civic_ID"):
    """
    The civic_join function joins the civic address data to the permit data.
        One civic may be joined to multiple permits, requiring a one-to-many join. Both features participating in the
        join, LND_civic_address and possee_permits, must be in the same workspace to accommodate ths type of join.

    :param permits_join_field:
    :param permits: Join the civic addresses to the permits
    :param output_workspace: Specify the location where the output will be saved
    :return: A tuple of two feature classes
    """

    permits_feature_name = arcpy.Describe(permits).name

    lnd_civ_address_local = arcpy.FeatureClassToFeatureClass_conversion(
        in_features=LND_CIVIC_ADDRESS,
        out_path=output_workspace,
        out_name="LND_civic_address"
    )[0]

    print(f"\nChecking civic addresses for matches to our permits...")

    civics_feature_layer = arcpy.MakeFeatureLayer_management(
        lnd_civ_address_local,
        # LND_CIVIC_ADDRESS,
        "civic_address_layer"
    ).getOutput(0)

    # Join needs to be made with LND_civic_address feature in same gdb as joining feature to maintain many-to-one relationship in export

    # Join Field gp tool doesn't seem to do many to one joins.

    permits_w_civics_layer = arcpy.AddJoin_management(
        in_layer_or_view=civics_feature_layer,
        in_field="CIV_ID",
        join_table=permits,
        join_field=permits_join_field,
        join_type="KEEP_COMMON"
    ).getOutput(0)

    print(f"\tJoin complete.")

    joined_permits_fields = [f'{os.path.basename(lnd_civ_address_local)}.Shape'] + [
        x.name for x in arcpy.ListFields(permits_w_civics_layer) if
        "OBJECTID" not in x.name.upper() and permits_feature_name in x.name
    ]  # July 4 - changes to use lnd_civ_address_local
    # Get fields associated with permits
    joined_data = [row for row in arcpy.da.SearchCursor(permits_w_civics_layer, joined_permits_fields)]

    # Create feature class to hold data
    joined_feature = arcpy.CreateFeatureclass_management(
        out_path=output_workspace,
        out_name=f"{permits_feature_name}_civic_joined",
        geometry_type="POINT",
        spatial_reference=arcpy.Describe(LND_CIVIC_ADDRESS).spatialReference,
        template=permits
    )[0]

    # Append records with data
    insert_fields = ["SHAPE@XY"] + [x.name for x in arcpy.ListFields(joined_feature) if
                                    x.name.upper() not in ("OBJECTID", "SHAPE")]
    with arcpy.da.InsertCursor(joined_feature, insert_fields) as cursor:
        for row in joined_data:
            cursor.insertRow(row)

    print("\tExporting joined data...")

    arcpy.Delete_management(permits_w_civics_layer)

    # This contains 810 duplicate Civic IDs for Building Permits
    all_permit_civ_ids = [row[0] for row in arcpy.da.SearchCursor(permits, permits_join_field) if row[0]]

    # joined_permits_w_civic_ids = [row[0] for row in arcpy.da.SearchCursor(joined_data, f"{permits_feature_name}_{permits_join_field}")]
    joined_permits_w_civic_ids = [row[0] for row in arcpy.da.SearchCursor(joined_feature, [permits_join_field])]

    # Figure out which civics were not joined
    civ_ids_failed_join = list(set([x for x in all_permit_civ_ids if x not in joined_permits_w_civic_ids]))

    # Get un-joined data
    no_join_sql = "1=2"

    if civ_ids_failed_join:
        no_join_sql = f"{permits_join_field} IN ({', '.join([str(x) for x in civ_ids_failed_join])})"

    print("\tExporting un-joined data...")
    unjoined_data = arcpy.TableSelect_analysis(
        permits,
        os.path.join(output_workspace, f"{permits_feature_name}_civid_unjoined"),
        no_join_sql
    ).getOutput(0)

    print(f"\tJoined to civics count: {arcpy.GetCount_management(joined_feature)[0]}")
    print(f"\tJoined to civics FAILED count: {arcpy.GetCount_management(unjoined_data)[0]}")

    arcpy.Delete_management(permits_w_civics_layer)

    return unjoined_data, joined_feature


def retire_civ_join(permits, output_workspace, permits_join_field: str = "Civic_ID"):
    """
    - For one to many joins to work, the features participating in the join must be in the same geodatabase - make sure
        unjoined permits and retired civics are in the same geodatabase.

    :param permits_join_field:
    :param permits:
    :param output_workspace:
    :return:
    """

    permits_feature_name = arcpy.Describe(permits).name

    lnd_civ_retire_local = arcpy.FeatureClassToFeatureClass_conversion(
        in_features=RETIRED_CIVICS,
        out_path=output_workspace,
        out_name="LND_civic_address_retired"
    )[0]

    joined_feature = arcpy.CreateFeatureclass_management(
        out_path=output_workspace,
        out_name=f"{permits_feature_name}_retired_civic_joined",
        geometry_type="POINT",
        spatial_reference=arcpy.Describe(LND_CIVIC_ADDRESS).spatialReference,
        template=permits
    )[0]

    no_join_sql = "1 < 0"
    unjoined_data = arcpy.TableSelect_analysis(
        permits,
        os.path.join(output_workspace, f"{permits_feature_name}_retired_civ_join_fails"),
        no_join_sql
    ).getOutput(0)

    input_row_count = int(arcpy.GetCount_management(permits)[0])
    if input_row_count == 0:
        print(f"\t'{permits_feature_name}' feature has no rows! Skipping Retired Civic Join.")
        return unjoined_data, joined_feature

    print(f"\nChecking RETIRED civic addresses for matches to our permits that do not join to LND_civic_address...")
    # lnd_civ_retire_local = os.path.join(output_workspace, "LND_retire_civ")

    retired_civs_layer = arcpy.MakeFeatureLayer_management(
        lnd_civ_retire_local,
        # RETIRED_CIVICS,
        "retired_civic_address_layer"
    ).getOutput(0)

    retired_civic_permits_layer = arcpy.AddJoin_management(
        in_layer_or_view=retired_civs_layer,
        in_field="CIV_ID",
        join_table=permits,
        join_field=permits_join_field,
        join_type="KEEP_COMMON"
    ).getOutput(0)

    print(f"\tJoin complete.")

    joined_permits_fields = [f'{os.path.basename(lnd_civ_retire_local)}.Shape'] + [
        x.name for x in arcpy.ListFields(retired_civic_permits_layer) if
        "OBJECTID" not in x.name.upper() and permits_feature_name in x.name
    ]  # Get fields associated with permits
    joined_data = [row for row in arcpy.da.SearchCursor(retired_civic_permits_layer, joined_permits_fields)]

    # Create feature class to hold data
    # Append records with data

    insert_fields = ["SHAPE@XY"] + [x.name for x in arcpy.ListFields(joined_feature) if
                                    x.name.upper() not in ("OBJECTID", "SHAPE")]
    with arcpy.da.InsertCursor(joined_feature, insert_fields) as cursor:
        for row in joined_data:
            cursor.insertRow(row)

    print("\tExporting joined data...")

    all_civic_ids = [row[0] for row in arcpy.da.SearchCursor(permits, permits_join_field) if row[0]]
    joined_permits_w_retired_civ_ids = [row[0] for row in arcpy.da.SearchCursor(retired_civic_permits_layer,
                                                                                f"{permits_feature_name}.{permits_join_field}")]

    # Figure out which civics were not joined
    retired_civ_ids_failed_join = [x for x in all_civic_ids if x not in joined_permits_w_retired_civ_ids]

    # Select None by default
    join_sql = "1 < 0"

    if all_civic_ids:
        join_sql = f"{permits_join_field} NOT IN ({', '.join([str(x) for x in all_civic_ids])})"
        no_join_sql = f"{permits_join_field} NOT IN ({', '.join([str(x) for x in all_civic_ids])})"

    if retired_civ_ids_failed_join:
        no_join_sql = f"{permits_join_field} IN ({', '.join([str(x) for x in retired_civ_ids_failed_join])})"

    if joined_permits_w_retired_civ_ids:
        join_sql = f"{permits_join_field} IN ({', '.join([str(x) for x in joined_permits_w_retired_civ_ids])})"

    print("\tExporting un-joined data...")
    unjoined_data = arcpy.TableSelect_analysis(
        permits,
        os.path.join(output_workspace, f"{permits_feature_name}_retired_civ_join_fails"),
        no_join_sql
    ).getOutput(0)

    print(f"\tJoined to retired civics count: {arcpy.GetCount_management(joined_feature)[0]}")
    print(f"\tJoined to retired civics FAILED count: {arcpy.GetCount_management(unjoined_data)[0]}")

    arcpy.Delete_management(retired_civic_permits_layer)

    return unjoined_data, joined_feature


def geocode_addresses(permits, workspace, locator: Geocoder, output_feature_name: str, score_minimum: int = 60,
                      address_field: str = "FORMATTEDADDRESS"):
    """
    The geocode_addresses function takes a feature class of permits and geocodes them.

    :param permits: Specify the input feature class
    :param workspace: Store the output of the geocode_addresses function
    :param locator: Geocoder: Specify the geocoder to use
    :param output_feature_name: str: Name the output feature class
    :param score_minimum: int: Set the minimum score that a geocoded address must have in order to be considered valid
    :param address_field: str: Specify the field in the permits feature class that contains the address to be geocoded

    :return: The geocoded features, the ungeocoded features, and the permits that are unable to be geocoded (because they have no address)
    """

    print(f"\nGeocoding Addresses in {permits}...")

    null_address_sql = f'{address_field} IS NULL'
    address_sql = f'{address_field} IS NOT NULL'

    print("\nGetting ungeocode-able permits...")
    ungeocodeable_permits = arcpy.TableSelect_analysis(
        permits,
        os.path.join(workspace, f"{output_feature_name}_ungeocodeable"),
        null_address_sql
    ).getOutput(0)

    # Get geocode-able features that have not already been joined
    # Geocode-able permits: Permits with a FORMATTEDADDRESS, not already joined to LND_civic_address or LND_retire_civ
    print("Getting geocode-able permits...")
    geocodable_permits = arcpy.TableSelect_analysis(
        permits,
        os.path.join(workspace, f"{output_feature_name}_geocodeable"),
        address_sql
    ).getOutput(0)

    num_geocodeable_permits = int(arcpy.GetCount_management(geocodable_permits)[0])
    print(f"\tNumber of geocode-able permits with a {address_field}: {num_geocodeable_permits}")

    # Geocode
    print("\nGeocoding addresses")
    geocoded, below_threshold = locator.geocode_addresses(
        # permits,
        geocodable_permits,
        workspace,
        output_feature_name,
        score_minimum,
        address_field
    )
    num_geocoded = arcpy.GetCount_management(geocoded)
    print(f"\tNumber of records geocoded: {num_geocoded}")

    num_below_threshold = arcpy.GetCount_management(below_threshold)
    print(f"\tNumber of records below minimum geocode score: {num_below_threshold}")

    return geocoded, below_threshold, ungeocodeable_permits


def get_parcel_geometry(parcel_fc, pid: str):
    """
    :param parcel_fc:
    :param pid:
    :return: (X, Y)
    """

    print(f"\nGetting parcel geometry for PID '{pid}'...")

    with arcpy.da.SearchCursor(parcel_fc, ["SHAPE@", "PID"], f"PID LIKE '{pid}'") as cursor:

        for row in cursor:

            parcel_geometry = row[0]
            parcel_id = row[1]

            # Calculate the centroid of the parcel
            center_point = parcel_geometry.trueCentroid  # Point(25562459.0570238 4951608.94148003 NaN NaN)

            print(f"PID: {parcel_id}")

            centroid_in_pid = parcel_geometry.contains(center_point)

            # Check if TRUECENTROID is within the parcel
            if centroid_in_pid:
                print(f"\tCentroid point for Parcel: {center_point.X}, {center_point.Y}")
                return center_point.X, center_point.Y

            if not centroid_in_pid:
                # Adjust coordinates if necessary
                # Can use Polygon labelPoint (The point at which the label is located. The labelPoint is always located within or on a feature.)
                # https://pro.arcgis.com/en/pro-app/3.0/arcpy/classes/polygon.htm

                label_point = parcel_geometry.labelPoint
                # adjusted_point = adjust_coordinates(parcel_geometry)
                print(f"\tAdjusted center point for Parcel: {label_point.X}, {label_point.Y}")
                return label_point.X, label_point.Y


def generate_pid_points(permits, output_workspace, reference_feature):
    """
    The generate_pid_points function takes a feature class or table with PIDs and creates a new point feature class
    using the centroid of the parcel that is associated with each PID.

    The function also returns two tables: one for records that did not have a PID, and one for records that had a PID
        but could not be located in the LND_PARCEL_POLYGON layer.

    :param permits: Generate the output feature class name
    :param output_workspace: Create a new feature class in the workspace
    :param reference_feature: Create the output feature class
    :return: A list of three items
    """

    # TODO: Update function to create new tables instead of editing existing

    records_desc = arcpy.Describe(permits)
    permits_feature_name = records_desc.name

    pid_sql = "PID IS NOT NULL"
    null_pid_sql = "PID IS NULL"

    print(f"\nGenerating points from PIDs for feature '{permits_feature_name}'...")

    # Make sure input feature has rows to process
    pid_rows = [row[0] for row in arcpy.da.SearchCursor(permits, ["PID"], pid_sql)]
    if not pid_rows:
        print(f"\t**NO ROWS WITH PID FOUND IN INPUT FEATURE: '{permits}'")

    # Get features without a PID - return this feature
    # TODO: this is redundant if only records with PIDs present are called with this function
    no_pid_records = arcpy.TableSelect_analysis(
        permits,
        os.path.join(output_workspace, f"{permits_feature_name}_no_PID"),
        null_pid_sql
    ).getOutput(0)

    # If input feature is a table, create as point feature class
    permits_feature = arcpy.CreateFeatureclass_management(
        out_path=output_workspace,
        out_name=f"{permits_feature_name}_pid_features",
        template=reference_feature,
        spatial_reference=arcpy.Describe(LND_CIVIC_ADDRESS).spatialReference,
        geometry_type="POINT"
    )[0]

    # Get features with PID
    # Populate rows of feature with input rows
    pid_records = arcpy.Append_management(
        inputs=permits,
        target=permits_feature,
        expression=pid_sql
    )[0]

    num_pid_records = int(arcpy.GetCount_management(pid_records)[0])
    print(f"\tRows with PID value: {arcpy.GetCount_management(pid_records)[0]}")

    unlocated_sql = "PID LIKE '-9999'"  # Select NO records
    unlocated_records = arcpy.MakeTableView_management(
        permits,
        "unlocated_records",
        where_clause=unlocated_sql
    )

    if num_pid_records > 0:
        unfound_pids = list()

        # Map PID records using centroid of PID. Make sure PIDs have leading zeros
        pids = [row[0] for row in arcpy.da.SearchCursor(pid_records, "PID")]

        pid_list_sql = ', '.join([f"'{x}'" for x in pids])
        sql = f"PID IN ({pid_list_sql})"

        # Build map of PID: (X, Y)
        centroids = {
            pid: get_parcel_geometry(LND_PARCEL_POLYGON, pid) for pid in pids
        }

        print(f"\n\tPopulating {pid_records} with centroid spatial data...")
        # with arcpy.da.UpdateCursor(pid_records, ["PID", "SHAPE@TRUECENTROID", 'Permit_Number']) as cursor:
        with arcpy.da.UpdateCursor(pid_records, ["PID", "SHAPE@XY",
                                                 'Permit_Number']) as cursor:  # TODO: Updated December 5 for testing
            for row in cursor:

                row_pid = row[0]
                update_centroid = centroids.get(row_pid)

                if update_centroid:
                    row[1] = update_centroid
                    cursor.updateRow(row)

                else:
                    print(f"\t\tNo centroid found for PID '{row_pid}', Permit Number: {row[2]}")
                    unfound_pids.append(row_pid)

                    # Remove row
                    cursor.deleteRow()

        del cursor

        pid_list_sql_fails = ', '.join([f"'{x}'" for x in unfound_pids])

        if pid_list_sql_fails:
            unlocated_sql = f"PID IN ({pid_list_sql_fails})"

        unlocated_records = arcpy.TableSelect_analysis(
            permits,
            os.path.join(output_workspace, f"{permits_feature_name}_has_PID_unlocated"),
            unlocated_sql
        ).getOutput(0)

    return pid_records, unlocated_records, no_pid_records


def generate_street_segment_points(permits, output_workspace, reference_feature):
    """
    - Locate permit to centroid of street segment if permit has an HRM_ID == street segment's FDMID
    :param permits:
    :param output_workspace:
    :param reference_feature:
    :return:
    """

    records_desc = arcpy.Describe(permits)
    permits_feature_name = records_desc.name

    hrm_id_sql = "HRM_ID IS NOT NULL"
    null_hrm_id_sql = "HRM_ID IS NULL"

    print(f"\nGenerating points from Street Segments for feature '{permits_feature_name}'...")

    # Get permit HRM_IDs
    hrm_ids = [row[0] for row in arcpy.da.SearchCursor(permits, ["HRM_ID"], hrm_id_sql)]
    if not hrm_ids:
        print(f"\t**NO ROWS WITH HRM_ID FOUND IN INPUT FEATURE: '{permits}'")

    no_hrm_id_records = arcpy.TableSelect_analysis(
        permits,
        os.path.join(output_workspace, f"{permits_feature_name}_no_PID"),
        null_hrm_id_sql
    ).getOutput(0)

    # If input feature is a table, create as point feature class
    permits_feature = arcpy.CreateFeatureclass_management(
        out_path=output_workspace,
        out_name=f"{permits_feature_name}_pid_features",
        template=reference_feature,
        spatial_reference=arcpy.Describe(LND_CIVIC_ADDRESS).spatialReference,
        geometry_type="POINT"
    )[0]

    # Get FDMIDs from HRM_IDs
    fdmids = [row[0] for row in arcpy.da.SearchCursor(TRN_STREET, "FDMID")]

    hrm_fdmids = [row[0] for row in arcpy.da.SearchCursor(permits, "HRM_ID", hrm_id_sql) if row[0] in fdmids]

    str_hrm_fdmids = [f"'{x}'" for x in hrm_fdmids]
    fdmid_sql = f"HRM_ID IN ({', '.join(str_hrm_fdmids)})"

    hrm_fdmid_records = arcpy.Append_management(
        inputs=permits,
        target=permits_feature,
        expression=fdmid_sql
    )[0]

    #####################
    num_hrm_fdmid_records = int(arcpy.GetCount_management(hrm_fdmid_records)[0])
    print(f"\tRows with FDMID value: {arcpy.GetCount_management(hrm_fdmid_records)[0]}")

    unlocated_sql = "HRM_ID LIKE '-9999'"  # Select NO records
    unlocated_records = arcpy.MakeTableView_management(
        permits,
        "unlocated_records",
        where_clause=unlocated_sql
    )

    if num_hrm_fdmid_records > 0:
        unfound_pids = list()

        # Map FDMID records using centroid of STREET SEGMENT.
        fdmids = [row[0] for row in arcpy.da.SearchCursor(hrm_fdmid_records, "HRM_ID")]

        sql = f"FDMID IN ({', '.join(fdmids)})"
        centroids = {
            row[0]: row[1] for row in arcpy.da.SearchCursor(
                TRN_STREET,
                ["FDMID", "SHAPE@TRUECENTROID"],
                where_clause=sql
            )
        }

        print(f"\n\tPopulating {hrm_fdmid_records} with centroid spatial data...")
        with arcpy.da.UpdateCursor(hrm_fdmid_records, ["HRM_ID", "SHAPE@TRUECENTROID", 'Permit_Number']) as cursor:
            for row in cursor:

                row_fdmid = row[0]
                update_centroid = centroids.get(row_fdmid)

                if update_centroid:
                    row[1] = update_centroid
                    cursor.updateRow(row)

                else:
                    print(f"\t\tNo centroid found for PID '{row_fdmid}', Permit Number: {row[2]}")
                    unfound_pids.append(row_fdmid)

                    # Remove row
                    cursor.deleteRow()

        del cursor

        fdmid_list_sql_fails = ', '.join([f"'{x}'" for x in unfound_pids])

        if fdmid_list_sql_fails:
            unlocated_sql = f"HRM_ID IN ({fdmid_list_sql_fails})"

        unlocated_records = arcpy.TableSelect_analysis(
            permits,
            os.path.join(output_workspace, f"{permits_feature_name}_has_FDMID_unlocated"),
            unlocated_sql
        ).getOutput(0)

    return hrm_fdmid_records, unlocated_records, no_hrm_id_records


def failed_locating_report(local_workspace, output_report):
    print("\nReporting on failed locates...")

    excel_writer = pd.ExcelWriter(output_report)

    dataframes = dict()

    with arcpy.EnvManager(workspace=local_workspace):

        # Get all _failed_locates features
        failed_locates_features = sorted(arcpy.ListTables("*failed_locates"))

        for feature in failed_locates_features:
            print(f"\n{feature}")
            print(f"\tRows: {arcpy.GetCount_management(feature)}")

            # Convert table to a Pandas DataFrame
            feature_data = [row for row in arcpy.da.SearchCursor(feature, "*")]
            temp_df = pd.DataFrame(feature_data, columns=[x.name for x in arcpy.ListFields(feature)])

            dataframes[feature] = temp_df

    # Iterate over the dictionary and write each DataFrame to a new sheet
    for table_name, dataframe in dataframes.items():
        sheet_name = table_name.replace("LND_PPLC_", "").replace("_failed_locates", "_fails")
        dataframe.to_excel(excel_writer, sheet_name=sheet_name, index=False)

    # Save the Excel file only if at least one sheet was written.
    # Closing an empty workbook raises: IndexError: At least one sheet must be visible
    if dataframes:
        excel_writer.close()  # The save() method has been deprecated and removed in Pandas. You should use close() instead.

    else:
        print("No failed locate tables found — skipping Excel report.")

    return output_report


def trunc_load_ro(SDEADM_RW, sde_ro):

    with arcpy.EnvManager(workspace=sde_ro, preserveGlobalIds=False):

        load_features = [PERMIT_INFO_FEATURE] + LND_PPLC_FEATURES

        # DELETE ROWS
        print("\nDeleting rows in features")

        for feature in load_features:
            print(f"\nFeature: {os.path.join(sde_ro, feature)}")

            row_count = int(arcpy.GetCount_management(feature)[0])
            print(f"\tPre-delete rows Row count: {row_count}")

            if row_count > 0:
                print(f"\t\tDeleting rows...")
                arcpy.DeleteRows_management(feature)

        # LOAD ROWS
        print("\nLOADING rows in features...")

        for feature in load_features:
            print(f"Feature: {os.path.join(sde_ro, feature)}")

            row_count = int(arcpy.GetCount_management(feature)[0])

            if row_count == 0:
                rw_feature = os.path.join(SDEADM_RW, feature)

                arcpy.Append_management(
                    inputs=rw_feature,
                    target=feature,
                    schema_type="TEST"
                )
                print("\tRows loaded.")


def remove_duplicates_from_csv(csv_file) -> pd.DataFrame:
    print(f"\nRemoving duplicates from '{csv_file}'...")

    df = pd.read_csv(csv_file, encoding_errors="replace")  # Dec 1. 2025 update

    og_row_count = df.shape[0]

    df_no_dups = df.drop_duplicates()
    new_row_count = df_no_dups.shape[0]

    rows_removed = og_row_count - new_row_count
    print(f"\tRemoved {rows_removed} rows.")

    df_no_dups.to_csv(csv_file, index=False)

    return csv_file, rows_removed