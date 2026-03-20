import arcpy


def get_schema_features(workspace, schema="HWADM"):
    hwadm_datasets = arcpy.ListDatasets(f"*{schema}*")
    hwadm_feature_classes = arcpy.ListFeatureClasses(f"*{schema}*")

    for dataset in hwadm_datasets:
        dataset_features = arcpy.ListFeatureClasses(wild_card=f"*{schema}.*", feature_dataset=dataset)

        hwadm_feature_classes.extend(dataset_features)

    hwadm_tables = arcpy.ListTables(f"*{schema}*")

    hwadm_features = [x.replace("GISRW01.", "") for x in hwadm_feature_classes + hwadm_tables]

    return hwadm_features

