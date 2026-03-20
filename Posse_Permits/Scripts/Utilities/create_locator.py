import arcpy

arcpy.env.overwriteOutput = True

output_locator = r"E:\HRM\Scripts\Python3\Posse_Permits\scripts\locator_test"
if arcpy.Exists(output_locator):
    arcpy.Delete_management(output_locator)

# arcpy.geocoding.CreateLocator(
#     "CAN",
#     r"E:\HRM\Scripts\Python3\Posse_Permits\scripts\data.gdb\LND_civic_address PointAddress",
#     "PointAddress.HOUSE_NUMBER LND_civic_address.FULL_CIVIC';PointAddress.STREET_NAME LND_civic_address.STR_NAME';",
#     output_locator,
#     "ENG",
#     None,
#     None,
#     None,
#     "LOCAL_EXTRA_HIGH"
# )

field_map = ("'PointAddress.HOUSE_NUMBER LND_civic_address.FULL_CIVIC';"
             "'PointAddress.STREET_NAME LND_civic_address.STR_NAME';"
             "'PointAddress.DISTRICT LND_civic_address.DISTRICT';"
             "'PointAddress.STREET_SUFFIX_TYPE LND_civic_address.STR_TYPE';"
             "'PointAddress.SUB_ADDRESS_UNIT LND_civic_address.UNIT_NUM';"
             "'PointAddress.CITY LND_civic_address.GSA_NAME';"
             "'PointAddress.SUBREGION LND_civic_address.MUN_CODE';"
             "'PointAddress.POSTAL LND_civic_address.CIV_POSTAL'")

arcpy.geocoding.CreateLocator(
    "CAN",
    r"E:\HRM\Scripts\Python3\Posse_Permits\scripts\data.gdb\LND_civic_address PointAddress",
    field_map,
    output_locator,
    "ENG",
    None,
    None,
    None,
    "LOCAL_EXTRA_HIGH"
)

arcpy.geocoding.CreateLocator(
    country_code="CAN",
    primary_reference_data=r"E:\HRM\Scripts\Python3\Posse_Permits\scripts\data.gdb\LND_civic_address PointAddress",
    field_mapping=field_map,
    out_locator=output_locator,
    language_code="ENG",
    alternatename_tables=None,
    alternate_field_mapping=None,
    custom_output_fields=None,
    # precision_type="GLOBAL_HIGH"
    precision_type="LOCAL_EXTRA_HIGH"
)

# arcpy.geocoding.CreateLocator(
#     "CAN", r"'E:\HRM\Scripts\SDE\SQL\qa_RW_sdeadm.sde\GISRW01.SDEADM.LND_civic_address' PointAddress",
#     """PointAddress.HOUSE_NUMBER 'GISRW01.SDEADM.LND_civic_address'.FULL_CIVIC"";""PointAddress.STREET_NAME 'GISRW01.SDEADM.LND_civic_address'.STR_NAME"";""PointAddress.STREET_SUFFIX_TYPE 'GISRW01.SDEADM.LND_civic_address'.STR_TYPE"";""PointAddress.CITY 'GISRW01.SDEADM.LND_civic_address'.GSA_NAME""", r"E:\HRM\Scripts\Python3\Posse_Permits\scripts\locator_test", "ENG", None, None, None, "LOCAL_EXTRA_HIGH")