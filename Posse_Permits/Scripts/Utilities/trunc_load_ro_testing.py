import arcpy
import os

# RW_SDE = r"C:\Users\alex.gallagher\AppData\Roaming\Esri\ArcGISPro\Favorites\prod_RW_sdeadm.sde"
# RO_SDE = r"E:\HRM\Scripts\SDE\prod_RO_sdeadm.sde"
RW_SDE = r"E:\HRM\Scripts\SDE\qa_RW_sdeadm.sde"
# RO_SDE = r"E:\HRM\Scripts\SDE\qa_RO_sdeadm.sde"

# Truncate RO & Load with RW

with arcpy.EnvManager(workspace=RO_SDE):

    feature_dataset = "SDEADM.LND_PPLC_Permits"

    pplc_features = arcpy.ListFeatureClasses(feature_dataset=feature_dataset)
    pplc_features = [os.path.join(feature_dataset, x) for x in pplc_features] + ["SDEADM.LND_PPLC_Permit_Info"]
    print(pplc_features)

    for feature in pplc_features:

        # Truncate RO feature
        print(f"\nTruncating '{feature}'...")

        if feature == "SDEADM.LND_PPLC_Permit_Info":
            arcpy.TruncateTable_management(feature)

        else:
            arcpy.DeleteFeatures_management(feature)

        # Load with RW feature
        load_feature = os.path.join(RW_SDE, feature)

        print(f"Loading '{feature}'...")
        arcpy.Append_management(
            inputs=load_feature,
            target=os.path.join(arcpy.env.workspace, feature),
            schema_type="TEST"
        )

    print(pplc_features)


