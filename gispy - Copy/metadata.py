import os
import arcpy
import re
import html

import pandas as pd

from arcpy import metadata as md

from datetime import datetime

arcpy.env.overwriteOutput = True
arcpy.SetLogHistory(False)


def strip_html_tags(html_text):

    # Replace HTML entities with their corresponding characters
    text = html.unescape(html_text)

    # Remove HTML tags using a regular expression
    clean_text = re.sub('<[^<]+?>', '', text)

    return clean_text


def get_xml_text(xml, tags='reviseDate'):

    # Get Revised date from xml
    match_found = re.search(rf'<{tags}>(.*?)</{tags}>', xml)

    if match_found:
        found_text = match_found.group(1)
        return found_text

class SDSFMetaData:

    xl_tab = "SDSF"

    def __init__(self, source):
        self.source = source

        self.df = pd.read_excel(source, sheet_name=SDSFMetaData.xl_tab)

        self.name = self.get_name()

        self.description = self.get_description()
        self.summary = self.get_summary()
        self.tags = self.get_tags()

        self.limitations = self.get_limitations()

    def __repr__(self):
        return self.name

    def get_description(self):
        header_idx = self.df.loc[self.df['Spatial Data Submission Form'] == 'Dataset Description:'].index[0]
        desc = self.df.iloc[header_idx + 1, 0]  # row, col
        return desc

    def get_summary(self):
        header_idx = self.df.loc[self.df['Spatial Data Submission Form'] == 'Dataset Purpose:'].index[0]
        desc = self.df.iloc[header_idx + 1, 0]  # row, col
        return desc

    def get_tags(self):
        header_idx = self.df.loc[self.df['Spatial Data Submission Form'] == 'Dataset Tags:'].index[0]
        tags = self.df.iloc[header_idx + 1, 0]  # row, col
        return tags

    def get_limitations(self):
        header_idx = self.df.loc[self.df['Spatial Data Submission Form'] == 'Notes or Disclaimers:'].index[0]
        limits = self.df.iloc[header_idx + 1, 0]  # row, col
        return limits

    def get_name(self):
        header_idx = self.df.loc[self.df['Spatial Data Submission Form'] == 'Dataset Name:'].index[0]
        name = self.df.iloc[header_idx, 1]  # row, col
        return f"METADATA: {name}"


def get_sde_metadata(db, feature):

    feature_name = arcpy.Describe(os.path.join(db, feature)).baseName.replace("GISRW01.SDEADM.", "")

    meta_data_info = {
        'FEATURE': feature.replace("GISRW01.", ""),
        'TITLE': '',
        'DESCRIPTION': '',
        'DESCRIPTION_SANITIZED': '',
        'TAGS': '',
        'SUMMARY': '',
        'CREATION_DATE': '',
        'PUBLISHED_DATE': '',
        'REVISION_DATE': '',
    }

    print(f"\nGetting metadata for {feature}...")

    # Create a metadata editor object
    metadata = md.Metadata(os.path.join(db, feature))

    # Update the metadata
    if metadata.title:
        meta_data_info['TITLE'] = metadata.title
    else:
        meta_data_info['TITLE'] = f"{feature_name} (*NO TITLE in Metadata*)"

    if metadata.description:

        meta_data_info['DESCRIPTION'] = metadata.description
        meta_data_info['DESCRIPTION_SANITIZED'] = strip_html_tags(metadata.description)

    if metadata.summary:
        meta_data_info['SUMMARY'] = metadata.summary

    if metadata.tags:
        meta_data_info['TAGS'] = metadata.tags

    # Get Created data from xml

    crea_date_match = get_xml_text(metadata.xml, 'CreaDate')

    if crea_date_match:
        meta_data_info['CREATION_DATE'] = crea_date_match

    # Get Revised date from xml
    revised_date_match = get_xml_text(metadata.xml, 'pubDate')

    if revised_date_match:
        meta_data_info['REVISION_DATE'] = revised_date_match

    # PUBLISHED DATE
    # Get Revised date from xml
    pub_date_match = get_xml_text(metadata.xml, 'reviseDate')

    if pub_date_match:
        meta_data_info['PUBLISHED_DATE'] = pub_date_match

    # Clean up the metadata editor
    del metadata

    return meta_data_info


def update_metadata(db, feature, update_options: dict):

    # Create a metadata editor object
    metadata = md.Metadata(os.path.join(db, feature))

    read_only = metadata.isReadOnly
    print(f"\tRead only: {read_only}")

    if not read_only:

        if update_options.get('revised_date'):

            current_revised_date = get_xml_text(metadata.xml, 'reviseDate')
            new_revised_date = update_options['revised_date']

            print(f"\tUpdating Revised date from {current_revised_date} to {new_revised_date}...")
            metadata.xml = metadata.xml.replace(
                f"<reviseDate>{current_revised_date}</reviseDate>",
                f"<reviseDate>{new_revised_date}</reviseDate>"
            )
        #
        # # Update the metadata
        # metadata.title = feature
        #
        # metadata.description = metadata.description
        # metadata.summary = metadata.summary
        # metadata.tags = metadata.tags

        # TODO: Type [Enterprise Geodatabase Feature Class, SDE Table, ]
        #  Use SDSF Class to get shape type/table
        #  Create subclass to track metadata information?

        # metadata.accessConstraints = metadata_info.limitations

        # Save the changes
        print("\n\tSaving update...")
        metadata.save()

    # Clean up the metadata editor
    del metadata


def get_workspace_features(workspace, schema=None) -> list:
    """
    - Get all features, including those in feature datasets.
    :param workspace:
    :return:
    """

    arcpy.env.workspace = workspace

    workspace_features = []

    print(f"\nGetting datasets from '{workspace}'...")

    if not schema:
        schema = ""

    datasets = arcpy.ListDatasets(f'*{schema}*', 'feature') + ['']

    # Get feature classes in feature datasets
    for fds in datasets:

        if fds:
            print(f"\nDATASET: {fds}")

        for fc in arcpy.ListFeatureClasses(f'*{schema}*', '', fds):

            data_path = os.path.join(fds, fc)

            workspace_features.append(data_path)

            print(f"\t{fc}")

    # Get workspace tables
    print("\nTABLES:")
    for tbl in arcpy.ListTables(f'*{schema}*'):
        workspace_features.append(tbl)

        print(f"\t{tbl}")

    return sorted(workspace_features)


if __name__ == "__main__":
    from configparser import ConfigParser

    config = ConfigParser()
    config.read("config.ini")

    SDE = config.get("LOCAL", "prod_rw")

    if "GIS" in os.environ.get("COMPUTERNAME").upper():
        SDE = config.get("SERVER", "prod_rw")

    print(datetime.now())

    # TODO: Update me
    sdsf = r"T:\work\giss\monthly\202403mar\gallaga\LND_ANS_communities\SDSform_LND_ANS_communities.xlsx"
    sheet_name = "DATASET DETAILS"

    # metadata_info = SDSFMetaData(sdsf)

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
            config.get("SERVER", "prod_rw"),  # qa_ro, qa_web_ro will get copied to db when processing rw
        ],

    ]:

        for count, db in enumerate(dbs, start=1):
            print(f"\n{count}/{len(dbs)}) Database: {db}")

            with arcpy.EnvManager(workspace=db):

                # # Get features
                # feature_classes = sorted(arcpy.ListFeatureClasses(wild_card="*LND*"))
                # num_features = len(feature_classes)

                # feature = 'SDEADM.LND_ANS_communities'
                # feature = 'SDEADM.LND_park_recreation_feature'
                # feature = os.path.join('SDEADM.LND_parcels', 'SDEADM.LND_parcel_polygon')

                # features = ['SDEADM.LND_ANS_communities', ]

                features = get_workspace_features(db, "SDEADM")
                features = sorted(
                    [
                        x for x in features if not x.endswith("_SAP") and
                                               not x.startswith("AR_") and
                                               not x.endswith("_temp") and
                                               not x.endswith("_H") and
                                               "SDE_VAT" not in x and
                                               "SDEADM.AR_" not in x and
                                               "SDEADM.SAP_" not in x and
                                               "SDEADM.TMP_" not in x and
                                               "DirtyObjects" not in x
                    ]
                )
                num_features = len(features)

                sde_metadata = list()

                for feature_count, feature in enumerate(features, start=1):
                    print(f"\n{feature_count}/{num_features}) Feature {feature}")

                    feature_metadata = get_sde_metadata(db, feature)
                    sde_metadata.append(feature_metadata)

                df = pd.DataFrame(sde_metadata)

                # Export each sde feature's metadata to a different row in a spreadsheet

                # Export to r"T:\work\giss\monthly\202403mar\gallaga\metadata\scripts\test_exports"

                output_dir = r"T:\work\giss\monthly\202403mar\gallaga\metadata\scripts\test_exports"

                # Export to csv
                output_csv = os.path.join(output_dir, f"sde_metadata.csv")

                df.to_csv(output_csv, index=False)

    print(datetime.now())
