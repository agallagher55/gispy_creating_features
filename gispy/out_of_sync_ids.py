import arcpy
import os
import re
import logging

from datetime import datetime

from gispy import utils

logFile = os.path.join(
    os.getcwd(),
    f"{datetime.today().date()}_prod_updating_features.log"
)
logger = utils.setupLog(logFile)

console_handler = logging.StreamHandler()
log_formatter = logging.Formatter(
    '%(asctime)s | %(levelname)s | FUNCTION: %(funcName)s | Msgs: %(message)s', datefmt='%d-%b-%y %H:%M:%S'
)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)  # logger.info logs to console

arcpy.SetLogHistory(False)
arcpy.env.overwriteOutput = True


def get_id_fields(feature):
    ...


def get_out_of_sync_ids(feature):
    ...


def get_id_sequence_names(id_field, feature, workspace):

    print(f"Getting name of sequence used for ID field '{id_field}'...")

    with arcpy.EnvManager(workspace=workspace):

        # Get attribute rule information
        feature_rules = arcpy.Describe(feature).attributeRules

        sequence_rule = [
            x for x in feature_rules if id_field.upper() == x.fieldName.upper() and "Sequence" in x.scriptExpression
        ]

        if sequence_rule:

            sequence_rule = sequence_rule[0]

            pattern = r"NextSequenceValue\('sdeadm\.(\w+)'\)"
            match = re.search(pattern, sequence_rule.scriptExpression)

            if match:
                sequence_name = match.group(1)

                return sequence_name


def update_id_field(sde, feature_name, csv_file, field_name_source, field_name_target, field_prefix=''):

    # TODO: Get editor tracking fields by using arcpy.Describe().EditorTracking
    # TODO: accommodate field prefix

    feature = os.path.join(sde, feature_name)

    desc = arcpy.Describe(feature)

    desc_editor_tracking = desc.editorTrackingEnabled
    logger.info(f"Editor Tracking: {desc_editor_tracking}")

    # Get attribute rules
    feature_rules = arcpy.Describe(feature).attributeRules
    delete_rules = [x.name for x in feature_rules if "GENERATE ID" in x.name.upper()]

    # Export Attribute Rules
    arcpy.ExportAttributeRules_management(feature, csv_file)

    # TODO: stop services/Remove locks
    # input("stop services/Remove locks")

    # Delete attribute Rules
    logger.info("Deleting Attribute Rules...")
    arcpy.DeleteAttributeRule_management(
        feature,
        delete_rules,
        "CALCULATION"
    )

    if desc_editor_tracking:
        creator_field = desc.creatorFieldName
        created_at_field = desc.createdAtFieldName

        editor_field = desc.editorFieldName
        editor_at_field = desc.editedAtFieldName
        time_utc = desc.isTimeInUTC

        # Remove editor tracking
        logger.info("Disabling Editor Tracking...")
        arcpy.DisableEditorTracking_management(
            feature,
            "DISABLE_CREATOR",
            "DISABLE_CREATION_DATE",
            "DISABLE_LAST_EDITOR",
            "DISABLE_LAST_EDIT_DATE"
        )

    # TODO: Make changes
    logger.info(f"Setting field {field_name_target} equal to {field_name_source}")
    expression = f"!{field_name_source}!" if not field_prefix else f"'{field_prefix}' + str(!{field_name_source}!)"
    arcpy.CalculateField_management(
        feature,
        field_name_target,
        expression,
        "PYTHON3",
        '',
        "TEXT",
        "NO_ENFORCE_DOMAINS"
    )

    if desc_editor_tracking:

        # Enable editor tracking
        logger.info("Enabling Editor Tracking...")
        arcpy.EnableEditorTracking_management(
            feature,
            creator_field,
            created_at_field,
            editor_field,
            editor_at_field,
            "NO_ADD_FIELDS",
            "UTC"
        )

    # Import Attribute Rules
    logger.info("Importing Attribute Rules...")
    arcpy.ImportAttributeRules_management(
        feature,
        csv_file
    )

    # TODO: Restart services
    logger.info("Restart Services")
    input("Restart Services")


if __name__ == "__main__":

    # SDE = r"E:\HRM\Scripts\SDE\SQL\qa_RW_sdeadm.sde"
    # SDE = r"E:\HRM\Scripts\SDE\SQL\Dev\dev_RW_sdeadm.sde"
    SDE = r"E:\HRM\Scripts\SDE\SQL\Prod\prod_RW_sdeadm.sde"

    feature_classes = {

        # "SDEADM.AST_amenity": ["ASSETID", "AMENITYID"],
        # "SDEADM.AST_bike_feature": ["ASSETID", "BIKEID"],

        "SDEADM.LND_hrm_parcel": [
            "ASSET_ID",  # Current ID sequence: (hrmparcelid)
            "ASSETID"  # Current ID sequence: (hrmparcelassetid)
        ],

        # "SDEADM.LND_outdoor_rec_poly": [
        #     "ASSETID",  # Current ID sequence:  (outdoorrecassetid)
        #     "RECPOLYID"  # Current ID sequence: (outdoorrecid)
        # ],

    }

    # csv_file = r"T:\work\giss\monthly\202306jun\gallaga\Updating Sequences\attribute_rules\ast_amenity.CSV"
    # field_name_source = "ASSETID"
    # field_name_target = "AMENITYID"

    # TODO: Check current sequence of IDs
    # LND_outdoor_rec_poly - sequences reset in prod
    # LND_hrm_parcel - sequences reset in prod

    logger.info(f"DATABASE: {SDE}")

    try:

        for feature_class in feature_classes:

            logger.info("="
                        *
                        50)
            logger.info(f"Feature Class: {feature_class}")

            csv_file = os.path.join(os.getcwd(), f"{feature_class.replace('SDEADM.', '')}.csv")
            field_name_source, field_name_target = feature_classes.get(feature_class)

            attribute_rule_names = ""

            # T:\work\giss\monthly\202401jan\gallaga\kirk
            # for sequence in feature_classes.get(feature_class):
            #     logger.info(f"Sequence field: {sequence}")
            #
            #     sequence_name = get_id_sequence_names(sequence, feature_class, SDE)
            #     logger.info(f"\tDatabase sequence name ==> {sequence_name}")

            update_id_field(
                SDE,
                feature_class,
                csv_file,
                field_name_source,
                field_name_target
            )

    except arcpy.ExecuteError:
        logger.error(f"ARCPY ERROR: {arcpy.GetMessages(2)}")