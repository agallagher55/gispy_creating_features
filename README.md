# GISpy - GIS Feature Creation Tool

A Python automation tool for creating and managing feature classes in Esri ArcGIS Enterprise Spatial Database Engine (SDE) environments. Designed for Halifax Regional Municipality (HRM) GIS operations.

## Overview

GISpy automates the workflow of creating new feature classes and tables in enterprise geodatabases. It reads feature schema definitions from an Excel-based Spatial Data Submission Form (SDSF), then orchestrates the full setup pipeline: domains, fields, versioning, replication, indexing, editor tracking, and attribute rules.

A companion geolocate script populates existing feature classes with spatial data from Data Warehouse staging tables, locating each record at its associated parcel geometry.

## Features

- **Excel-driven schema**: Define feature classes entirely via Excel submission forms
- **Multi-environment support**: Dev, QA, and Production SDE databases
- **Automated domain management**: Create and transfer coded value domains
- **Feature replication**: Sync features between read-write and read-only SDE instances
- **Editor tracking**: Auto-configure audit fields (ADDBY, MODBY, ADDDATE, MODDATE)
- **Attribute rules**: Auto-increment sequence rules on ID fields
- **Subtype support**: Feature classes with per-subtype domain assignments
- **Privilege management**: Configurable user/role permissions
- **Versioning**: Automatic SDE versioning registration
- **Archiving**: Optional SDE archiving support
- **Geolocation**: Locate application records at parcel centroids or unioned parcel polygons

## Requirements

- ArcGIS Pro or ArcGIS Enterprise with a valid ArcPy license
- Python 3.x (bundled with ArcGIS Pro)
- `pandas` and `openpyxl` libraries
- Access to MSSQL-backed SDE geodatabases
- Windows environment (paths and SDE connections are Windows-specific)

## Installation

1. Clone this repository into your scripts directory:
   ```
   git clone <repo-url> pplc_applications
   ```

2. Install required libraries if not already available in your ArcGIS Python environment:
   ```
   pip install pandas openpyxl
   ```

3. Configure your SDE connection files (`.sde`) for Dev, QA, and Production environments.

## Configuration

### `config.ini`

Edit the `[SERVER]` section to point to your SDE connection files and
`[HRM_DW]` to point to the Data Warehouse staging connection:

```ini
[SERVER]
dev_rw  = E:\path\to\dev_RW_sdeadm.sde
dev_ro  = E:\path\to\dev_RO_sdeadm.sde
qa_rw   = E:\path\to\qa_RW_sdeadm.sde
qa_ro   = E:\path\to\qa_RO_sdeadm.sde
prod_rw = E:\path\to\prod_RW_sdeadm.sde
prod_ro = E:\path\to\prod_RO_sdeadm.sde

[HRM_DW]
connFileDev = E:\path\to\dw_staging_dev.sde
```

### `feature_config_planning_applications.ini`

Configure each feature creation job:

```ini
[SDSF_SETTINGS]
sdsf = T:\path\to\SpatialDataSubmissionForm.xlsx
sheet_name = "DATASET DETAILS"
SDSF_IGNORE_FIELDS = ["OBJECTID", "GLOBALID", "SHAPE", "SHAPE_AREA", "SHAPE_LENGTH"]

[FEATURE_SETTINGS]
add_editor_tracking = False
enable_archiving = False
EDIT_PERMISSIONS_USERS = []
ready_to_add_to_replica = False
replica_name = LND_Rosde
subtypes = False
topology_dataset = False
subtype_field =
subtype_domains = {}

[UNIQUE_ID_FIELDS]
; Map feature_name to a JSON list of {field, prefix} dicts
; lnd_pplc_planning_applications = [{"field": "APP_ID", "prefix": "PA-"}]

[NEW_DOMAIN_TYPES]
; Uncomment and define domain field types as needed:
; LND_my_domain = TEXT
```

## Usage

### Create a new feature class

Run the main script to create a new feature class based on your configured SDSF:

```bash
python create_feature_planning_applications.py
```

The script will:
1. Parse the Excel SDSF to extract feature name, geometry type, fields, and domains
2. Create coded value domains in the target workspace
3. Create the feature class with specified geometry type
4. Add all fields with types, lengths, domains, and default values
5. Add GlobalIDs and configure editor tracking (if enabled)
6. Register as versioned in SDE; enable archiving (if enabled)
7. Copy the RW feature class to RO SDE (unversioned, editor tracking disabled)
8. Add the feature to synchronization replicas (if enabled)
9. Create indexes on ID fields (RW and RO)
10. Apply attribute rules for auto-incrementing IDs

### Geolocate planning application features

Run the geolocate script to populate existing SDE feature classes from Data
Warehouse staging tables, locating each record at its parcel geometry:

```bash
python geolocate_features.py
```

Configure `geolocate.ini` before running:

```ini
[GEOLOCATE]
dw_source_tables = [
    ("OPENDATA_SOURCE.PPLC_PLANNING_APPLICATIONS", "SDEADM.LND_PPLC_planning_applications"),
    ]
pid_field        = PID
truncate_and_load = True

[PID_FIELDS]
; Optional: override the PID field name per source table
; OPENDATA_SOURCE.PPLC_PLANNING_APPLICATIONS = PIDs
```

Two geometry modes are supported (set `GEOMETRY_MODE` in the script):
- **`POLYGON`** (default): All parcel polygons for an application are unioned into one dissolved polygon. Use when records reference multiple PIDs.
- **`POINT`**: A point is placed at the centroid of the primary (first) PID's parcel.

The script will:
1. Read records from Data Warehouse staging tables
2. Separate records by PID availability
3. Look up parcel geometry from `LND_parcel_polygon` for each unique PID
4. Export located records to CSV; create temp feature class in scratch workspace
5. Append attributes and update geometry via UpdateCursor
6. Load located features into the target SDE feature class (truncate-and-load)
7. Replicate updated features from RW to RO SDE
8. Generate an Excel report of records that could not be located

## Project Structure

```
pplc_applications/
├── create_feature_planning_applications.py      # Feature class creation entry point
├── geolocate_features.py                        # Geolocate features from DW staging
├── gispy_utils.py                               # Shared helpers: load_to_sde(), replicate_to_ro()
├── config.ini                                    # Server SDE and DW connection paths
├── feature_config_planning_applications.ini      # Feature-specific job settings
├── geolocate.ini                                 # Geolocate job settings
│
├── Logs/                                         # Runtime log files (auto-created)
├── Scratch/                                      # Scratch file geodatabases (auto-created)
├── Reports/                                      # Failure/locate Excel reports (auto-created)
├── Exports/                                      # CSV exports for arcpy Append (auto-created)
│
├── Posse_Permits/                                # Posse permit processing scripts
│   └── Scripts/                                  # ETL and utility scripts
│
└── gispy/                                        # Core library package
    ├── attribute_rules.py                        # Attribute rules and sequences
    ├── connections.py                            # SDE/GDB connection utilities
    ├── domains.py                                # Coded domain management
    ├── editor_tracking.py                        # Editor tracking field setup
    ├── features.py                               # Generic feature operations
    ├── metadata.py                               # Metadata extraction and update
    ├── metadata_update.py                        # Standalone metadata update script
    ├── subtypes.py                               # Subtype configuration
    ├── utils.py                                  # create_fgdb(), setupLog(),
    │                                             #   table_to_dataframe(), build_field_mapping()
    ├── out_of_sync_ids.py                        # ID sync between RW and RO SDE
    ├── list_schema_features.py                   # Schema feature listing
    ├── project.py                                # Project utilities
    │
    ├── SpatialDataSubmissionForms/               # Excel SDSF parsing module
    │   ├── features.py                           # Feature class creation wrapper
    │   ├── reporter.py                           # Excel report parser
    │   ├── main.py                               # Alternate entry point
    │   ├── submission_form.py                    # Submission form models
    │   └── settings.py                           # Module settings
    │
    ├── replicas/                                 # Replication management
    │   ├── replicas.py                           # Core replica operations
    │   ├── replicas_qa.py                        # QA/testing replica operations
    │   └── examples/                             # Usage examples
    │
    └── attrubute_rules/                          # Attribute rules reporting (note: intentional typo)
        └── reporting.py
```

## Notes

- **SDE ID fields** must be set to `NULLABLE` for Registry Editor services to create features and calculate IDs post-creation.
- **Credentials**: Do not commit `.sde` files or `config.ini` files containing passwords to version control.
- **Runtime directories** (`Logs/`, `Scratch/`, `Reports/`, `Exports/`) are created automatically when scripts run.
- The tool is tailored for HRM dataset naming conventions (LND, ADM, AST, CIV, BLD, EMO, MAP, ROAD, SNF, StrDir, TRN).
- `gispy_utils.py` re-exports `table_to_dataframe` and `build_field_mapping` from `gispy.utils` for backward compatibility.
