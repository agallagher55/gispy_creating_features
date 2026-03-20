import arcpy
import os

SDE = r"E:\HRM\Scripts\SDE\qa_RW_sdeadm.sde"


with arcpy.EnvManager(workspace=SDE):
    permit_features = arcpy.ListFeatureClasses(feature_dataset="SDEADM.LND_PPLC_Permits")
    print(permit_features)

    text_fields = [x.name for x in arcpy.ListFields(permit_features[0]) if x.type == "String"]

    for feature in permit_features:
        print(feature)

        arcpy.DeleteFeatures_management(feature)

        # Alter text fields
        # Permit_Name
        for field in text_fields:
            print(f"\t{field}")

            try:
                arcpy.AlterField_management(
                    in_table=feature,
                    field="PERMIT_NAME",
                    field_type="TEXT",
                    field_length=256
                )

            except arcpy.ExecuteError:
                print(arcpy.GetMessages(2))

