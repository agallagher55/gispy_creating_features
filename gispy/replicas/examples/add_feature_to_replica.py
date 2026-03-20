import arcpy
import os

from replicas import add_to_replica

if __name__ == "__main__":

    dev_rw = r"E:\HRM\Scripts\SDE\SQL\Dev\dev_RW_sdeadm.sde"
    dev_ro = r"E:\HRM\Scripts\SDE\SQL\Dev\dev_RO_sdeadm.sde"

    qa_rw = r"E:\HRM\Scripts\SDE\SQL\qa_RW_sdeadm.sde"
    qa_ro = r"E:\HRM\Scripts\SDE\SQL\qa_RO_sdeadm.sde"

    prod_rw = r"E:\HRM\Scripts\SDE\SQL\Prod\prod_RW_sdeadm.sde"
    prod_ro = r"E:\HRM\Scripts\SDE\SQL\Prod\prod_RO_sdeadm.sde"

    for rw_sde, ro_sde in (
            # (dev_rw, dev_ro),
            (qa_rw, qa_ro),
            # (prod_rw, prod_ro),
    ):

        # replica_name = "LND_Rosde"
        # replica_name = "ADM_Rosde"
        replica_name = "TRN_Rosde"

        topology_dataset = False

        # current_workspace = replicas.Workspace(rw_sde)
        # workspace_replicas = [x.name for x in current_workspace.replicas]

        # replica_features = replicas.Replica(replica_name, rw_sde).datasets
        #
        # # Write current replica features
        # with open(f"{replica_name}.txt", "w") as txtfile:
        #     for feature in sorted(list(set(replica_features))):
        #         txtfile.write(f"{feature}\n")

        new_features = [

            "SDEADM.TRN_STREET_RETIRED"

            # "SDEADM.ADM_community_planning_program",
            # "SDEADM.FLD_fp_SackRivers_100yr_v1",
            # "SDEADM.FLD_fp_SackRivers_100yr_v2",
            # "SDEADM.FLD_fp_SackRivers_20yr_v1",
            # "SDEADM.FLD_fp_SackRivers_20yr_v2"
        ]

        # all_features = replica_features + new_features

        # replicas.add_to_replica(
        #     replica_name=replica_name,
        #     rw_sde=rw_sde,
        #     ro_sde=ro_sde,
        #     # add_features=all_features,
        #     add_features=new_features,
        #     topology_dataset=True
        # )

        add_to_replica(
            replica_name=replica_name,
            rw_sde=rw_sde,
            ro_sde=ro_sde,
            add_features=new_features,
            topology_dataset=topology_dataset,
            # feature_dataset="FLD_FP_SackvilleRivers"
        )

        # CHECKS
        # RW is Versioned
        # RW & RO have GlobalIDs
