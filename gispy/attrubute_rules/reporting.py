import os
import arcpy
import logging
import datetime

import attr_rules

from configparser import ConfigParser

from hrmutils.HRMutils import setupLog

arcpy.env.overwriteOutput = True
arcpy.SetLogHistory(False)

config = ConfigParser()
config.read('config.ini')

logFile = os.path.join(os.getcwd(), f"{datetime.date.today()}_loggies.log")
logger = setupLog(logFile)

console_handler = logging.StreamHandler()
log_formatter = logging.Formatter(
    '%(asctime)s | %(levelname)s | FUNCTION: %(funcName)s | Msgs: %(message)s', datefmt='%d-%b-%y %H:%M:%S'
)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)  # print logs to console
logger.setLevel(logging.DEBUG)

if __name__ == "__main__":

    separator = 79 * "="

    for dbs in [
        # [utils.create_fgdb(out_folder_path=CURRENT_DIR, out_name="scratch.gdb")],
        [
            config.get("SERVER", "dev_ro"),
        ],
        # [
        #     config.get("SERVER", "qa_ro"),
        # ],
        # [
        #     config.get("SERVER", "prod_ro"),
        # ],

    ]:

        for count, db in enumerate(dbs, start=1):

            logger.info(f"{count}/{len(dbs)}) Database: {db}")

            with arcpy.EnvManager(workspace=db):

                features_with_rules = list()
                features = arcpy.ListFeatureClasses()
                tables = arcpy.ListTables()
                datasets = arcpy.ListDatasets()

                for dataset in datasets:
                    dataset_features = arcpy.ListFeatureClasses(feature_dataset=dataset)
                    features.extend(dataset_features)

                all_features = features + tables

                num_features = len(all_features)

                for count, feature in enumerate(all_features, start=1):
                    print(separator)
                    print(f"\n{count}/{num_features}) {feature}")
                    print(f"\tGetting rules...")

                    rules = attr_rules.get_rules(feature)

                    if rules:
                        print("\t\tRules found!!")
                        print(rules)
                        features_with_rules.append(feature)

                if features_with_rules:
                    print("\nFeatures with rules:")

                for feature in features_with_rules:
                    print(f"{feature}")
