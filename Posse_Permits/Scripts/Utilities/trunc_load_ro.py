import os
import arcpy

arcpy.env.preserveGlobalIds = True
arcpy.SetLogHistory(False)

PERMIT_INFO_FEATURE = "SDEADM.LND_PPLC_Permit_Info"

LND_PPLC_FEATURES = [
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_Building_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_Construction_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_Engineering_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_HW_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_LU_Approval_Permits",
    "SDEADM.LND_PPLC_Permits\SDEADM.LND_PPLC_PW_ROW_Permits",
]


def trunc_load_ro(sde_rw, sde_ro):
    with arcpy.EnvManager(workspace=sde_ro):

        # DELETE ROWS
        print("\nDeleting rows in features")

        for feature in [PERMIT_INFO_FEATURE] + LND_PPLC_FEATURES:
            print(f"\nFeature: {feature}")

            row_count = int(arcpy.GetCount_management(feature)[0])
            print(f"\tPre-delete rows Row count: {row_count}")

            if row_count > 0:
                arcpy.DeleteRows_management(feature)

        # LOAD ROWS
        print("\nLoading rows in features")

        for feature in [PERMIT_INFO_FEATURE] + LND_PPLC_FEATURES:
            print(f"\nFeature: {feature}")

            row_count = int(arcpy.GetCount_management(feature)[0])
            if row_count == 0:
                append_feature = os.path.join(sde_rw, feature)

                arcpy.Append_management(
                    inputs=append_feature,
                    target=feature,
                    schema_type="TEST"
                )


if __name__ == "__main__":
    SDE_RW = r"E:\HRM\Scripts\SDE\SQL\qa_RW_sdeadm.sde"
    SDE_RW = r"E:\HRM\Scripts\SDE\SQL\Prod\prod_RW_sdeadm.sde"

    SDE_RO = r"E:\HRM\Scripts\SDE\SQL\qa_RO_sdeadm.sde"
    SDE_RO = r"E:\HRM\Scripts\SDE\SQL\Prod\prod_RO_sdeadm.sde"

    # TODO: Turn into a function and add to posse_permits.py
