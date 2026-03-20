import os

import pandas as pd

PERMITS_UPLOAD_DIR = r"\\msbidsp01\OUTPUT\OpenData\upload"

unique_rows_csv = r"E:\HRM\Scripts\Python3\Posse_Permits\scripts\exports\PPLC_Building_Permits_unique_records.csv"

local_permit_exports = [
    os.path.join(PERMITS_UPLOAD_DIR, "PPLC_Issued_Building_Permits.csv"),
    os.path.join(PERMITS_UPLOAD_DIR, "PPLC_Issued_Construction_Permits.csv"),
    os.path.join(PERMITS_UPLOAD_DIR, "PPLC_Issued_Engineering_Permits.csv"),  # Changed from Civic_ID (June 5)
    os.path.join(PERMITS_UPLOAD_DIR, "PPLC_Issued_Halifax_Water_Permits.csv"),
    os.path.join(PERMITS_UPLOAD_DIR, "PPLC_Issued_Land_Use_Approval_Permits.csv"),  # Changed from HRM_ID (June 27)
    os.path.join(PERMITS_UPLOAD_DIR, "PPLC_Issued_Public_Works_ROW_Permits.csv"),
]

for csv in local_permit_exports:
    print(f"\n{csv}")

    # Read csv in as a pandas DataFrame
    df = pd.read_csv(csv)

    # Get the length of each work description record - if field is null, set new COMMENTS_LENGTH field to 0
    df['WORK_DESC_LENGTH'] = df['Work_Description'].apply(lambda x: len(x) if pd.notnull(x) else 0)

    # Get the length of the largest work description
    max_description_length = df['WORK_DESC_LENGTH'].max()
    print(f"Max description length: {max_description_length}")

    # Current field length for work description is 2000 - needs to be bumped up to at least 4071 or need to truncate
    # work descriptions
