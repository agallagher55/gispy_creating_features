import os
import arcpy

from metadata import (
    get_sde_metadata,
    update_metadata,
)

from configparser import ConfigParser
from datetime import datetime

from gispy import (
    SpatialDataSubmissionForms
)

arcpy.env.overwriteOutput = True
arcpy.SetLogHistory(False)

config = ConfigParser()
config.read('config.ini')

SDE = config.get("LOCAL", "prod_rw")

if "GIS" in os.environ.get("COMPUTERNAME").upper():
    SDE = config.get("SERVER", "prod_rw")


if __name__ == "__main__":
    print(datetime.now())

    CURRENT_DIR = os.getcwd()

    for dbs in [
        # [utils.create_fgdb(out_folder_path=CURRENT_DIR, out_name="scratch.gdb")],
        # [
        #     config.get("SERVER", "dev_rw"),
        # ],
        # [
        #     config.get("SERVER", "qa_rw"),  # qa_ro, qa_web_ro will get copied to db when processing rw
        # ],
        [
            # config.get("SERVER", "prod_rw"),  # qa_ro, qa_web_ro will get copied to db when processing rw
            config.get("SERVER", "prod_ro"),  # qa_ro, qa_web_ro will get copied to db when processing rw
        ],

    ]:

        DATE_TODAY = datetime.today().strftime('%Y-%m-%dT00:00:00')

        update_attributes = {
            "revised_date": DATE_TODAY
        }

        for count, db in enumerate(dbs, start=1):
            print(f"\n{count}/{len(dbs)}) Database: {db}")

            with arcpy.EnvManager(workspace=db):

                # features = [
                #     # os.path.join('SDEADM.LND_parcels', 'SDEADM.LND_parcel_polygon'),
                #     # os.path.join('SDEADM.LND_parcels', "SDEADM.LND_parcel_line"),
                #     # "SDEADM.LINNS_ALL",
                #     "SDEADM.LINNS_PIDAANTAX",
                #     "SDEADM.LINNS_PIDMSTRS",
                #     "SDEADM.LINNS_PIDNAMES",
                #     "SDEADM.LINNS_PIDRELATE",
                #     "SDEADM.LND_ghosted_parcel_line",
                #     "SDEADM.LND_parcel_point",
                # ]

                # RO
                features = [
                    os.path.join('SDEADM.LND_parcels', 'SDEADM.LND_parcel_polygon'),
                    os.path.join('SDEADM.LND_parcels', "SDEADM.LND_parcel_line"),
                ]

                num_features = len(features)

                for feature_count, feature in enumerate(features, start=1):
                    print(f"\n{feature_count}/{num_features}) Feature {feature}")

                    feature_metadata = get_sde_metadata(db, feature)

                    # Update Metadata
                    revised_date = feature_metadata.get("REVISION_DATE")

                    if revised_date:
                        update_metadata(db, feature, update_attributes)

                print()

    print(datetime.now())
