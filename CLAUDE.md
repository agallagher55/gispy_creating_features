# CLAUDE.md — Development Guide for AI Assistants

This file provides context and guidelines for AI assistants (Claude) working on this codebase.

## Project Summary

**GISpy** automates creation of ArcGIS Enterprise feature classes from Excel-based Spatial Data Submission Forms (SDSF). It targets Halifax Regional Municipality's SDE geodatabases (MSSQL backend) across Dev, QA, and Production environments.

The codebase depends on **ArcPy** (Esri's Python library), which requires a licensed ArcGIS Pro or ArcGIS Enterprise installation. The tool is Windows-only.

## Repository Layout

```
gispy_creating_features/
├── create_new_feature_planning_applications.py  # Primary script — run this
├── geolocate_features.py                        # Geolocate features from DW staging
├── config.ini                                    # SDE and DW connection file paths
├── geolocate.ini                                 # Per-run geolocate settings
├── feature_config_planning_applications.ini      # Per-job feature settings
│
└── gispy/                                        # Core importable package
    ├── attribute_rules.py     # add_sequence_rule() — creates auto-increment rules
    ├── connections.py         # connection_type() — detects SDE vs GDB, RW vs RO
    ├── domains.py             # transfer_domains(), domains_in_db()
    ├── editor_tracking.py     # Adds ADDBY/MODBY/ADDDATE/MODDATE fields
    ├── features.py            # Generic feature class utilities
    ├── metadata.py            # Metadata extraction and update
    ├── subtypes.py            # create_subtype() with domain assignments
    ├── utils.py               # create_fgdb(), query_all_feature(), decorators
    ├── out_of_sync_ids.py     # ID sync between RW and RO SDE
    ├── list_schema_features.py
    ├── project.py
    │
    ├── SpatialDataSubmissionForms/
    │   ├── features.py        # Feature class — primary wrapper for feature creation
    │   ├── reporter.py        # Report, FieldsReport, DomainsReport (Excel parsers)
    │   ├── submission_form.py # SDSF data models
    │   ├── main.py            # Alternate entry point
    │   └── settings.py        # Module settings
    │
    ├── replicas/
    │   ├── replicas.py        # Replica, sync_replicas(), add_to_replica()
    │   └── examples/          # Standalone usage examples (not imported)
    │
    └── attrubute_rules/       # Note: intentional typo in folder name
        └── reporting.py
```

## Key Workflows

### Feature Creation Pipeline

The main script (`create_new_feature_planning_applications.py`) drives this sequence:

1. Load `config.ini` and `feature_config_planning_applications.ini`
2. Parse Excel SDSF → extract feature name, geometry type, fields, domains
3. Create coded value domains (`domains.py`)
4. Create feature class with geometry type
5. Add all field definitions (type, length, domain, default, alias)
6. Add GlobalIDs; configure editor tracking if enabled
7. Register as SDE versioned
8. Add to sync replica if `ready_to_add_to_replica = True`
9. Create indexes on ID fields
10. Add attribute rules / sequences

### Geolocate Features Pipeline

The geolocate script (`geolocate_features.py`) drives this sequence:

1. Load `config.ini` (SDE + DW connections) and `geolocate.ini`
2. Read records from Data Warehouse staging tables
3. Separate records: those with a valid PID vs. those without
4. Look up parcel centroids from `LND_parcel_polygon` for each unique PID
5. Create point features in scratch workspace using the target SDE feature as template
6. Load located features into the target SDE feature class (truncate-and-load)
7. Replicate updated features from RW to RO SDE
8. Write an Excel report of unlocated records (no PID or PID not in parcel layer)

### Configuration Loading

- `config.ini` → SDE connection file paths (server-side: `[SERVER]`, local dev: `[LOCAL]`) and Data Warehouse staging connection (`[HRM_DW]`)
- `feature_config_planning_applications.ini` → per-run settings (SDSF path, domains, replicas)
- `geolocate.ini` → geolocate job settings (`dw_source_tables`, `pid_field`, `truncate_and_load`)
- All use Python's `configparser`; values are read with `config.get(section, key)`

## Development Guidelines

### Do

- Run and test using **ArcGIS Pro's Python environment** (`python.exe` bundled with ArcGIS Pro), not system Python — ArcPy is only available there.
- Keep feature creation steps modular: each concern (domains, fields, replicas, rules) lives in its own module.
- Follow HRM dataset naming conventions: `<SCHEMA>_<THEME>_<NAME>` (e.g., `LND_PPLC_planning_applications`).
- Use `configparser` for all configuration; avoid hardcoding connection paths.
- Keep `config.ini` entries as file paths to `.sde` connection files; never embed raw credentials in Python code.
- Adhere to python pep8 style guide

### Do Not

- Do not hardcode SDE paths or passwords in Python files.
- Do not commit `config.ini` files containing real credentials to version control — add them to `.gitignore` if needed.
- Do not run the main script against Production without confirming `ready_to_add_to_replica`, editor tracking, and privilege settings.
- Do not rename the `attrubute_rules/` directory — it may be imported elsewhere under that spelling.

## Important Notes

### ArcPy Dependency

All core modules import `arcpy`. Any linting or static analysis will flag these as unresolved unless run inside the ArcGIS Pro Python environment. Do not attempt to resolve `arcpy` imports via pip.

### ArcGIS Pro 3.3.5 deployment

### SQL Server Enterprise Geodatabase

### SDE ID Fields Must Be Nullable

SDE ID fields (e.g., sequence-generated IDs) must be `NULLABLE`. Registry Editor services create features first, then calculate IDs; non-nullable ID fields will cause errors.

### Multi-Environment Pattern

The same script targets Dev → QA → Prod by swapping the SDE connection path. The `connections.py` module provides `connection_type()` to detect read-write vs. read-only and SDE vs. file GDB connections.

### No Test Suite

There are currently no automated tests. When adding functionality:
- Test manually against a Dev SDE environment first.
- Use the `examples/` directory in `replicas/` as a pattern for manual integration tests.

## Common Tasks

### Add a new feature class

1. Fill out the Excel SDSF with field definitions, geometry type, and domains.
2. Update `feature_config_planning_applications.ini` with the SDSF path and feature settings.
3. Run `python create_new_feature_planning_applications.py`.

### Add a new domain

1. Add the domain field type to `[NEW_DOMAIN_TYPES]` in the feature config INI.
2. Define domain values in the Excel SDSF.
3. The `domains.py` module will pick them up during the run.

### Add feature to a replica

1. Set `ready_to_add_to_replica = True` in the feature config.
2. Set `replica_name` to the target replica name.
3. Re-run the script (it will skip already-created steps if coded defensively).

### Run the geolocate script

1. Ensure `config.ini` has a valid `[HRM_DW]` connection pointing to the staging database.
2. Configure `geolocate.ini` with `dw_source_tables` (list of `(dw_table, target_feature)` tuples), `pid_field`, and `truncate_and_load`.
3. Run `python geolocate_features.py`.
4. Check the Reports directory for the failure report if any records could not be located.

## Dependencies

| Library | Source | Purpose |
|---------|--------|---------|
| `arcpy` | ArcGIS Pro install | All GIS operations |
| `pandas` | pip / conda | Excel SDSF parsing and reporting |
| `configparser` | Python stdlib | INI config files |
| `os`, `ast`, `datetime` | Python stdlib | Path, literal eval, and date utilities |
