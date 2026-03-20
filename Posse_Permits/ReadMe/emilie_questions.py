import arcpy
import os

import pandas as pd

from pprint import pprint

SDE_RW = r"E:\HRM\Scripts\SDE\SQL\Prod\prod_RW_sdeadm.sde"
# SDE_RW = r"E:\HRM\Scripts\SDE\SQL\qa_RW_sdeadm.sde"

SDE_RO = r"E:\HRM\Scripts\SDE\SQL\Prod\prod_RO_sdeadm.sde"
# SDE_RO = r"E:\HRM\Scripts\SDE\SQL\qa_RW_sdeadm.sde"

FAILED_LOCATES_XLSX = r"E:\HRM\Scripts\Python3\Posse_Permits\scripts\failed_permit_locates_prod.xlsx"
# FAILED_LOCATES_XLSX = r"E:\HRM\Scripts\Python3\Posse_Permits\scripts\failed_permit_locates.xlsx"

RELATED_TABLE = os.path.join(SDE_RW, "GISRW01.SDEADM.LND_PPLC_Permit_Info")

BI_OUTPUT_DIR = r"\\ms-bi-app-p22\OUTPUT\OpenData\upload"

BI_CSVS = [
    "PPLC_Issued_Building_Permits.csv",
    "PPLC_Issued_Construction_Permits.csv",
    "PPLC_Issued_Engineering_Permits.csv",
    "PPLC_Issued_Halifax_Water_Permits.csv",
    "PPLC_Issued_Land_Use_Approval_Permits.csv",
    "PPLC_Issued_Public_Works_ROW_Permits.csv",
]

PERMIT_INFO_FEATURE = "SDEADM.LND_PPLC_Permit_Info"

LND_PPLC_FEATURES = [
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_Building_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_Construction_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_Engineering_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_HW_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_LU_Approval_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_PW_ROW_Permits",
]


def permits_with_occupancy(df):
    return df[df['Occupancy_Type'].notna()]


def permits_not_in_related_table(permits_df, related_table_df):
    unfound_permits = permits_df[~permits_df['Permit_Number'].isin(related_table_df['Permit_Number'])]['Permit_Number'].tolist()
    print(f"Permits not in related table: {len(unfound_permits)}")
    return unfound_permits


def multi_pid_permits_no_occupancy(df):
    # Filter where occupancy_type is null
    df_null_occupancy = df[df['Occupancy_Type'].isnull() & df['PID'].notna()].sort_values(by="Permit_Number")

    # Group by PID
    df_null_occupancy['PID_Count'] = df_null_occupancy.groupby('Permit_Number')['PID'].transform('nunique')

    # Get groups with more than one record
    result_df = df_null_occupancy[df_null_occupancy['PID_Count'] > 1].sort_values(by="Permit_Number")

    permits = result_df['Permit_Number'].unique().tolist()
    print(f"Number of rows with an null Occupancy Type and multiple PIDs: {len(permits)}")

    return result_df


def process_statistics(rw_sde, ro_sde):
    """
    - Get counts of rows in rw, ro, source data, fails

    :param rw_sde:
    :param ro_sde:
    :return:
    """

    results = {
        "RW": {},
        "RO": {},
        "BI": {},
        "FAILS": {},
        "MISSING_RW": {},
        "MISSING_RO": {}
    }

    print("\nProcessing row counts for BI...")
    for csv in sorted(BI_CSVS):
        csv_file = os.path.join(BI_OUTPUT_DIR, csv)
        df = pd.read_csv(csv_file)
        row_count = df.shape[0]
        results["BI"][os.path.basename(csv_file)] = row_count

    sde_features = sorted([PERMIT_INFO_FEATURE] + LND_PPLC_FEATURES)

    # RW Counts
    print(f"\nProcessing row counts for RW...")
    with arcpy.EnvManager(workspace=rw_sde):

        for bi_csv in sde_features:

            row_count = int(arcpy.GetCount_management(bi_csv)[0])
            results["RW"][os.path.basename(bi_csv)] = row_count

    # RO Counts
    print(f"\nProcessing row counts for RO...")
    with arcpy.EnvManager(workspace=ro_sde):

        for bi_csv in sde_features:

            row_count = int(arcpy.GetCount_management(bi_csv)[0])
            results["RO"][os.path.basename(bi_csv)] = row_count

    print("\nProcessing row counts from fails report...")
    xl_tabs = pd.ExcelFile(FAILED_LOCATES_XLSX).sheet_names
    for tab in sorted(xl_tabs):
        df = pd.read_excel(FAILED_LOCATES_XLSX, sheet_name=tab)
        row_count = df.shape[0]
        results["FAILS"][tab] = row_count

    # Missing RW = BI - (RW + FAILS + related_table)
    # Get DataFrames: BI, RW, Fails, Related Table
    # See which Permit Numbers in BI are not in any of (RW, Fails, Related Table)
    # Get Permit_Number counts then cross-reference with other tables to see if all duplicates are accounted for

    related_table_fields = [x.name for x in arcpy.ListFields(RELATED_TABLE)]
    related_table_df = pd.DataFrame(
        [row for row in arcpy.da.SearchCursor(RELATED_TABLE, related_table_fields)],
        index=None,
        columns=related_table_fields,
    )

    for count, bi_csv in enumerate(results["BI"]):
        print("\n" + bi_csv)
        bi_df = pd.read_csv(os.path.join(BI_OUTPUT_DIR, bi_csv))
        print(f"\tBI row count: {bi_df.shape[0]}")

        rw_feature = list(results["RW"])[count + 1]
        rw_feature_path = os.path.join(SDE_RW, rw_feature)
        rw_feature_fields = [x.name for x in arcpy.ListFields(rw_feature_path)]
        rw_df = pd.DataFrame(
            [row for row in arcpy.da.SearchCursor(rw_feature_path, "*")],
            index=None,
            columns=rw_feature_fields
        )
        print(f"\tRW row count: {rw_df.shape[0]}")

        fails_df = pd.read_excel(FAILED_LOCATES_XLSX, sheet_name=xl_tabs[count])
        print(f"\tFails row count: {fails_df.shape[0]}")

        permit_category = ""
        if "Building" in bi_csv:
            permit_category = 'Building'

        elif "Construction" in bi_csv:
            permit_category = 'Construction'

        elif "Engineering" in bi_csv:
            permit_category = 'Engineering'

        elif 'Public_Works_ROW' in bi_csv:
            permit_category = 'Public_Works_ROW'

        elif 'Halifax_Water' in bi_csv:
            permit_category = 'Halifax_Water'

        elif 'Land_Use_Approval' in bi_csv:
            permit_category = 'Land_Use_Approval'

        related_table_filtered_df = related_table_df[related_table_df['Permit_Category'] == permit_category]
        print(f"\tRelated table row count: {related_table_filtered_df.shape[0]}")

        missing_permit_numbers = bi_df[
            ~bi_df['Permit_Number'].isin(rw_df['PERMIT_NUMBER']) &
            ~bi_df['Permit_Number'].isin(fails_df['Permit_Number']) &
            ~bi_df['Permit_Number'].isin(related_table_filtered_df['Permit_Number'])
            ]

        print(f"\t*Missing Permit Numbers: {len(missing_permit_numbers)}")

        if not missing_permit_numbers.empty:
            print(missing_permit_numbers.head())
            output_dir = r"E:\HRM\Scripts\Python3\Posse_Permits\scripts\qa_qc"
            out_qaqc_csv = bi_csv.replace(".csv", "_missing_permit_numbers.csv")
            missing_permit_numbers.to_csv(os.path.join(output_dir, out_qaqc_csv), index=False)

    # RO MISSING = RW - RO

    pprint(results)
    return results


def related_table_by_type():
    print(f"\nGetting permits in related table by Permit Category...")

    related_table_fields = [x.name for x in arcpy.ListFields(RELATED_TABLE)]

    related_table_df = pd.DataFrame(
        [row for row in arcpy.da.SearchCursor(RELATED_TABLE, related_table_fields)],
        index=None,
        columns=related_table_fields,
    )

    permit_category_counts = related_table_df["Permit_Category"].value_counts().to_dict()

    for category in permit_category_counts:
        print(f"\t{category}: {permit_category_counts[category]}")

    return permit_category_counts


if __name__ == "__main__":

    related_table_by_type()
    process_statistics(SDE_RW, SDE_RO)

    building_permits_src = os.path.join(BI_OUTPUT_DIR, "PPLC_Issued_Building_Permits.csv")
    construction_permits_src = os.path.join(BI_OUTPUT_DIR, "PPLC_Issued_Construction_Permits.csv")

    dtypes = {"PID": str, "Civic_Number": str}

    bldg_permits_df = pd.read_csv(building_permits_src, dtype=dtypes)
    construction_permits_df = pd.read_csv(construction_permits_src, dtype=dtypes)

    related_table_fields = [x.name for x in arcpy.ListFields(RELATED_TABLE)]
    related_table_df = pd.DataFrame(
        [row for row in arcpy.da.SearchCursor(RELATED_TABLE, related_table_fields)],
        index=None,
        columns=related_table_fields,
        # dtype=dtypes
    )

    ######################################################################################################
    # 1. Do all permits that have occupancy types have a record in the related table?
    ######################################################################################################
    print('\n' + "="*125)
    print("QUESTION 1) Do all permits that have occupancy types have a record in the related table?")
    print("="*125)
    bldg_permits_w_occupancy_df = permits_with_occupancy(bldg_permits_df)
    construction_permits_w_occupancy_df = permits_with_occupancy(construction_permits_df)

    print("Checking for BUILDING permits not in related table...")
    bldg_permit_permits_unfound = permits_not_in_related_table(bldg_permits_w_occupancy_df, related_table_df)

    print("\nChecking for CONSTRUCTION permits not in related table...")
    construction_permit_permits_unfound = permits_not_in_related_table(construction_permits_w_occupancy_df, related_table_df)

    ######################################################################################################
    # 2. Are permits that have multiple PIDs but no occupancy type showing up in the related table?
    # Get construction permits with multiple PIDs, but no Occupancy Type
    ######################################################################################################
    print('\n' + "="*125)
    print("QUESTION 2) Are permits that have multiple PIDs but no occupancy type showing up in the related table?")
    print("="*125)

    print("Getting BUILDING permits with no occupancy type, but have multiple PIDs...")
    multi_pid_building_permits = multi_pid_permits_no_occupancy(bldg_permits_df)

    print("\nGetting CONSTRUCTION permits with no occupancy type, but have multiple PIDs...")
    multi_pid_construction_permits = multi_pid_permits_no_occupancy(construction_permits_df)  # DECK-2022-11395

    print("\nChecking for BUILDING permits with no occupancy type, but have multiple PIDs that aren't in related table...")
    if not multi_pid_building_permits.empty:
        unfound_permits = permits_not_in_related_table(multi_pid_building_permits, related_table_df)

    else:
        print("\t*No records with no occupancy type, but multiple PIDs exist in the dataset!")

    print("\nChecking for CONSTRUCTION permits with no occupancy type, but have multiple PIDs that aren't in related table...")
    if not multi_pid_construction_permits.empty:
        unfound_permits = permits_not_in_related_table(multi_pid_construction_permits, related_table_df)

    else:
        print("\t*No records with no occupancy type, but multiple PIDs exist in the dataset!")


    """
    3. 
    One follow-up topic I’m wondering about is the function of the related table for the other permit categories 
        (Public Works, Halifax Water, Engineering, and Land Use Approval). None of these have occupancy types, 
        but some do have PIDs. Here’s what I’m seeing:
    
        Land Use Approval & Halifax Water – provides multiple records for permits with multiple PIDs (great!), 
        but also provides records for permits with a single PID, though not all of the permits with only a single PID. 
        Why are these latter records included if it’s some but not all of them?
        
        Engineering & Public Works – same as above, but also: there are many records with no PID at all, 
        so the only information included in the related table is the permit number and permit category. 
        In some cases there are multiple records for one permit with no other information in the table. 
        Why are these included and why does a single permit with no other information have 14 records in the table?
    
    """

