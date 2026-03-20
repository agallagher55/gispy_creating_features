import os
import arcpy
import ast

from gispy import (
    connections,
    attribute_rules
)

from gispy.replicas import replicas

from configparser import ConfigParser

from gispy.subtypes import create_subtype
from gispy.domains import transfer_domains, domains_in_db

from gispy.SpatialDataSubmissionForms.features import Feature
from gispy.SpatialDataSubmissionForms.reporter import FieldsReport, DomainsReport

arcpy.env.overwriteOutput = True
arcpy.SetLogHistory(False)

MAX_TABLE_NAME_LENGTH = 27

config = ConfigParser()
config.read('config.ini')

PROD_SDE = config.get("SERVER", "prod_rw")
SPATIAL_REFERENCE = os.path.join(PROD_SDE, "SDEADM.LND_hrm_parcel_parks", "SDEADM.LND_hrm_park")

VALID_FIELD_TYPES = {"TEXT", "SHORT", "LONG", "FLOAT", "DOUBLE", "DATE"}


def sort_key_description(row):
    val = row.Description

    # Put None at the end
    if val is None:
        return 2, ""

    # Try numeric first
    try:
        return 0, int(val)  # numeric bucket, sorted numerically

    except (TypeError, ValueError):
        return 1, str(val)  # non numeric, sorted alphabetically


def process_feature_config(feature_config_path: str) -> None:
    """Run the full feature creation pipeline for a single feature config file.

    Args:
        feature_config_path: Path to the feature config .ini file.
    """
    print(f"\n{'=' * 60}")
    print(f"Processing config: {feature_config_path}")
    print(f"{'=' * 60}")

    feature_config = ConfigParser()
    feature_config.optionxform = str  # preserve case
    feature_config.read(feature_config_path)

    # --- SDSF may be a single path or a comma-separated list of paths ---
    raw_sdsf = feature_config.get("SDSF_SETTINGS", "sdsf")
    sdsf_files = [p.strip() for p in raw_sdsf.split(",") if p.strip()]

    SDSF_IGNORE_FIELDS = ast.literal_eval(feature_config.get("SDSF_SETTINGS", "SDSF_IGNORE_FIELDS"))

    ADD_EDITOR_TRACKING = feature_config.getboolean("FEATURE_SETTINGS", "add_editor_tracking")
    EDIT_PERMISSIONS_USERS = ast.literal_eval(feature_config.get("FEATURE_SETTINGS", "EDIT_PERMISSIONS_USERS"))

    READY_TO_ADD_TO_REPLICA = feature_config.getboolean("FEATURE_SETTINGS", "ready_to_add_to_replica")
    REPLICA_NAME = feature_config.get("FEATURE_SETTINGS", "replica_name")

    SUBTYPES = feature_config.getboolean("FEATURE_SETTINGS", "subtypes")
    SUBTYPE_FIELD = feature_config.get("FEATURE_SETTINGS", "subtype_field", fallback="")
    SUBTYPE_DOMAINS = ast.literal_eval(feature_config.get("FEATURE_SETTINGS", "subtype_domains"))

    TOPOLOGY_DATASET = feature_config.getboolean("FEATURE_SETTINGS", "topology_dataset")

    NEW_DOMAIN_TYPES = dict(feature_config.items("NEW_DOMAIN_TYPES"))

    for domain, field_type in NEW_DOMAIN_TYPES.items():
        if field_type.upper() not in VALID_FIELD_TYPES:
            raise ValueError(f"Error: Field type '{field_type}' for domain '{domain}' is not standard.")

    if ADD_EDITOR_TRACKING:
        SDSF_IGNORE_FIELDS.extend(["ADDBY", "ADDDATE", "MODBY", "MODDATE"])

    for dbs in [
        [
            config.get("SERVER", "dev_rw"),
        ],

        # [
        #     config.get("SERVER", "qa_rw"),  # qa_ro will get copied to db when processing rw
        # ],

        # [
        #     config.get("SERVER", "prod_rw"),
        # ],

    ]:

        for count, db in enumerate(dbs, start=1):
            print(f"\n{count}/{len(dbs)}) Database: {db}")

            # Determine the type and read-write status of a database. Ex) SDE + RW, SDE + RO, GDB, etc.
            db_type, db_rights = connections.connection_type(db)

            for xl_file_num, xl_file in enumerate(sdsf_files, start=1):
                print(f"\n{xl_file_num}/{len(sdsf_files)}) Creating feature from {xl_file}...")
                fields_report = FieldsReport(xl_file)

                feature_name = fields_report.feature_class_name  # Should be all lower case except for the prefix
                feature_shape = fields_report.feature_shape

                UNIQUE_ID_FIELDS = ast.literal_eval(
                    feature_config.get("UNIQUE_ID_FIELDS", feature_name, fallback='[]')
                )

                if feature_shape.upper() == "LINE":
                    feature_shape = "Polyline"

                field_data = fields_report.field_details

                domains_report = DomainsReport(xl_file)

                domain_names, domain_dataframes = domains_report.domain_info()

                if db_type == "GDB":

                    # Transfer existing domains to local gdb and find new domains not in SDE
                    new_domains = transfer_domains(
                        domains=domain_names,
                        output_workspace=db,
                        from_workspace=PROD_SDE
                    ).get("unfound_domains")

                else:
                    # Check for new domains not found in sde
                    domains_in_sde, new_domains, db_domains = domains_in_db(db, domain_names)

                # Create any new domains
                if new_domains:
                    print(f"\nNew domains to create: {', '.join(new_domains)}")

                    subtype_domain_names = (
                        {d["domain"] for d in SUBTYPE_DOMAINS["domains"]} if SUBTYPE_DOMAINS else set()
                    )

                    for domain in new_domains:

                        try:
                            field_type = "TEXT"

                            if domain in NEW_DOMAIN_TYPES:
                                field_type = NEW_DOMAIN_TYPES.get(domain)

                            # Check if domain is a subtype domain
                            if domain in subtype_domain_names:
                                field_type = "LONG"
                                print("\t*Subtype Domain Found!")

                            print(f"\n\tCreating domain '{domain}'...")
                            arcpy.CreateDomain_management(
                                in_workspace=db,
                                domain_name=domain,
                                field_type=field_type,
                                domain_type="CODED",
                                domain_description="",
                                split_policy="DUPLICATE"
                            )
                            # Sometimes this says it 'fails', but domain still gets created

                        except arcpy.ExecuteError:
                            arcpy_msg = arcpy.GetMessages(2)
                            print(f"Arcpy Error: {arcpy_msg}")
                            print(f"^^^*(Sometimes this fails in the script, but domain still gets created.)")

                        domain_df = domain_dataframes.get(domain)

                        sort_key = (lambda x: x.Code) if domain in subtype_domain_names else sort_key_description

                        # TypeError: '<' not supported between instances of 'str' and 'int'
                        for row in sorted([x for x in domain_df.itertuples()], key=sort_key):
                            code = row.Code
                            desc = row.Description

                            print(f"\tAdding ({code}: {desc})")
                            arcpy.AddCodedValueToDomain_management(
                                in_workspace=db,
                                domain_name=domain,
                                code=code,
                                code_description=desc
                            )

                else:
                    print("\nNO new domains to create.")

                # Create the feature and add fields
                if (db_type == "SDE" and db_rights == "RW") or (db_type == "GDB" and not db_rights):

                    new_feature = Feature(
                        workspace=db,
                        feature_name=feature_name,
                        geometry_type=feature_shape,
                        spatial_reference=SPATIAL_REFERENCE
                    )

                    print("\nAdding Fields...")

                    for row_num, row in field_data.iterrows():

                        field_name = row["Field Name"].upper().strip()
                        field_length = row["Field Length"]

                        if field_name not in SDSF_IGNORE_FIELDS:
                            alias = row["Alias"]
                            field_type = row["Field Type"]
                            nullable = row["Nullable"]
                            default_value = row["Default Value"]
                            domain = row["Domain"] or "#"

                            if field_length:
                                field_length = int(field_length)

                            if field_type == "TEXT" and not field_length:
                                raise ValueError(
                                    f"Field {field_name} of type {field_type} needs to have a field length.")

                            new_feature.add_field(
                                field_name=field_name.upper(),
                                field_type=field_type,
                                length=field_length,
                                alias=alias,
                                # nullable=nullable,
                                domain_name=domain
                            )

                            if domain and domain != "#":
                                print(f"\t\t{field_name} has domain: '{domain}'")
                                new_feature.assign_domain(
                                    field_name=field_name,
                                    domain_name=domain,
                                    subtypes="#"
                                )

                            # Apply default values for fields, if applicable
                            if default_value:
                                new_feature.add_field_default(
                                    field=field_name,
                                    default_value=default_value
                                )

                    # ADD GLOBAL IDS
                    new_feature.add_globalids()

                    if ADD_EDITOR_TRACKING:
                        # ADD EDITOR TRACKING FIELDS
                        if db_type in ("SDE", "GDB") and db_rights in ("RW", ""):
                            new_feature.add_editor_tracking_fields()

                    # Update Privileges
                    if db_type != "GDB":
                        new_feature.change_privileges(
                            user="PUBLIC",
                            view="GRANT"
                        )

                        for user in EDIT_PERMISSIONS_USERS:
                            print(f"\nEnabling privileges for {user}")
                            arcpy.ChangePrivileges_management(
                                in_dataset=new_feature.feature,
                                user=user,
                                Edit="GRANT"
                            )

                    # SUBTYPES
                    if SUBTYPES:
                        create_subtype(new_feature.feature, SUBTYPE_FIELD, SUBTYPES, SUBTYPE_DOMAINS)

                    if db_type == "SDE" and db_rights == "RW":

                        # Register as Versioned
                        new_feature.register_as_versioned()  # needs to be versioned to add to replica

                        # COPY FEATURE TO RO
                        ro_sdeadm_db = db.replace("RW", "RO")
                        ro_sdeadm_feature = os.path.join(ro_sdeadm_db, new_feature.feature_name)

                        for ro_feature, ro_db in [(ro_sdeadm_feature, ro_sdeadm_db)]:

                            # Don't need to add to WEB if feature is a table
                            if feature_shape.upper() == 'ENTERPRISE GEODATABASE TABLE':
                                print(f"\nFeature is a table - skipping adding to WEB RO...")
                                continue

                            if not arcpy.Exists(ro_feature):
                                print(f"\tCopying RW feature to {ro_db}...")

                                if feature_shape.upper() in ('ENTERPRISE GEODATABASE TABLE', 'NOT APPLICABLE'):
                                    feature = arcpy.TableToTable_conversion(
                                        in_rows=new_feature.feature,
                                        out_path=ro_db,
                                        out_name=new_feature.feature_name
                                    )[0]

                                else:
                                    feature = arcpy.FeatureClassToFeatureClass_conversion(
                                        in_features=new_feature.feature,
                                        out_path=ro_db,
                                        out_name=new_feature.feature_name,
                                    )[0]

                        if READY_TO_ADD_TO_REPLICA:
                            replicas.add_to_replica(
                                replica_name=REPLICA_NAME,
                                rw_sde=db,
                                ro_sde=ro_sdeadm_db,
                                add_features=[new_feature.feature],
                                topology_dataset=TOPOLOGY_DATASET
                            )

                        # Un-version RO feature, disable editor tracking, index
                        for feature in [ro_sdeadm_feature]:

                            if arcpy.Exists(feature):

                                print(f"\tRegistering as UN-versioned for '{feature}'...")
                                arcpy.UnregisterAsVersioned_management(in_dataset=feature)

                                if ADD_EDITOR_TRACKING:
                                    print(f"\tDisabling Editor Tracking for '{feature}'...")
                                    arcpy.DisableEditorTracking_management(in_dataset=feature)

                                # Set privileges
                                ro_users = ["PUBLIC", "SDE"]

                                for user in ro_users:
                                    arcpy.ChangePrivileges_management(
                                        in_dataset=feature,
                                        user=user,
                                        View="GRANT"
                                    )

                                for field_info in UNIQUE_ID_FIELDS:
                                    id_field = field_info.get("field")

                                    print(f"\nAdding attribute index on {id_field}...")
                                    try:
                                        arcpy.AddIndex_management(
                                            in_table=feature,
                                            fields=id_field,
                                            index_name=f"index_{id_field}",
                                            ascending="ASCENDING"
                                        )

                                    except arcpy.ExecuteError:
                                        arcpy_msg = arcpy.GetMessages(2)
                                        print(arcpy_msg)

                    if ADD_EDITOR_TRACKING:
                        # ENABLE EDITOR TRACKING
                        new_feature.enable_editor_tracking()

                    # Attribute Rules - Add after feature has been copied to Read-Only. RW and .gdb only
                    for field_info in UNIQUE_ID_FIELDS:

                        id_field = field_info.get("field")
                        prefix = field_info.get("prefix")

                        print(f"Creating Sequence and Attribute Rule for {id_field} with prefix {prefix}...")

                        attribute_rules.add_sequence_rule(
                            workspace=db,
                            feature_name=new_feature.feature,
                            field_name=id_field,
                            sequence_prefix=prefix,
                        )

                        print(f"\nAdding attribute index on {id_field}...")
                        try:
                            arcpy.AddIndex_management(
                                in_table=new_feature.feature,
                                fields=id_field,
                                index_name=f"index_{id_field}",
                                ascending="ASCENDING"
                            )

                        except arcpy.ExecuteError:
                            arcpy_msg = arcpy.GetMessages(2)
                            print(arcpy_msg)

    # Checks:
    # Replicas
    # Indexes
    # Attribute Rules
    # Default values
    # Domains
    # Privileges assigned
    # Versioned
    # Editor Tracking
    # Features in RO, WEB_RO

    # Add to CMDB


if __name__ == "__main__":

    feature_config_files = [
        "feature_config_planning_applications.ini",
        # "feature_config_another_feature.ini",
    ]

    for i, cfg_path in enumerate(feature_config_files, start=1):
        print(f"\nConfig {i}/{len(feature_config_files)}: {cfg_path}")
        process_feature_config(cfg_path)
