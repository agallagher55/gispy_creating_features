import arcpy

import pandas as pd

arcpy.SetLogHistory(False)

workspace = r"E:\HRM\Scripts\Python3\Posse_Permits\scripts\scratch_III.gdb"

output_report = f"failed_permit_locates.xlsx"

if __name__ == "__main__":

    def failed_locating_report(local_workspace, output_report):

        excel_writer = pd.ExcelWriter(output_report)

        dataframes = dict()

        with arcpy.EnvManager(workspace=workspace):

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
        for sheet_name, dataframe in dataframes.items():
            dataframe.to_excel(excel_writer, sheet_name=sheet_name, index=False)

        # Save the Excel file
        excel_writer.save()

        return output_report
