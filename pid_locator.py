"""
OOP-based PID feature location engine.

Provides a composable pipeline for locating ArcGIS features using parcel
geometry from LND_parcel_polygon. Supports two geometry strategies:

- PolygonPIDLocator: unions all parcel polygons for each record's
  comma-separated PID list into a single dissolved polygon.
- PointPIDLocator: places a point at the centroid of the primary (first) PID's
  parcel polygon.

Typical usage::

    from gispy.pid_locator import (
        ParcelLookup, PolygonPIDLocator, FeatureLocator, GEOMETRY_MODES
    )

    parcel_lookup = ParcelLookup(parcel_fc)
    locator = GEOMETRY_MODES["POLYGON"](parcel_lookup)
    feature_locator = FeatureLocator(scratch_ws, spatial_ref, exports_dir, locator)

    located_fc, unlocated_df = feature_locator.locate(
        records_df=has_pid_df,
        target_feature=target_sde_feature,
        pid_field="PIDs",
        sde_pid_field="PID",
        field_map_dict={"SDE_FIELD": "DW_COLUMN"},
    )
"""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

import arcpy
import pandas as pd

from gispy.utils import build_field_mapping

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PID parsing helpers
# ---------------------------------------------------------------------------

def extract_all_pids(pid_value) -> list[str]:
    """
    Extract and zero-pad all PIDs from a potentially comma-separated PID string.

    :param pid_value: A single PID string or comma-separated PID list
    :return: List of zero-padded 8-character PID strings; empty list if
        empty/null
    """
    if not pid_value or pd.isna(pid_value):
        return []
    return [
        p.strip().zfill(8)
        for p in str(pid_value).split(",")
        if p.strip()
    ]


def extract_primary_pid(pid_value) -> Optional[str]:
    """
    Extract and zero-pad the first PID from a potentially comma-separated
    PID string.

    :param pid_value: A single PID string or comma-separated PID list
    :return: Zero-padded 8-character PID string, or None if empty/null
    """
    if not pid_value or pd.isna(pid_value):
        return None
    first = str(pid_value).split(",")[0].strip()
    return first.zfill(8) if first else None


# ---------------------------------------------------------------------------
# Parcel lookup
# ---------------------------------------------------------------------------

class ParcelLookup:
    """
    Fetches parcel geometry from LND_parcel_polygon by PID.

    Supports single-record lookups (get_polygon / get_centroid) and
    bulk pre-fetch (prefetch_polygons / prefetch_centroids) to avoid
    opening one cursor per PID across large record sets.

    :param parcel_fc: Path to the LND_parcel_polygon feature class
    """

    def __init__(self, parcel_fc: str):
        self._parcel_fc = parcel_fc

    def get_polygon(self, pid: str) -> Optional[arcpy.Geometry]:
        """
        Return the full polygon geometry for a single PID, or None.

        :param pid: Zero-padded 8-character PID string
        :return: arcpy Geometry or None if not found
        """
        with arcpy.da.SearchCursor(
            self._parcel_fc, ["SHAPE@"], f"PID LIKE '{pid}'"
        ) as cursor:
            for row in cursor:
                return row[0]
        return None

    def get_centroid(self, pid: str) -> Optional[tuple[float, float]]:
        """
        Return the centroid (X, Y) for a single PID, or None.

        Uses trueCentroid if it falls inside the parcel polygon, otherwise
        falls back to labelPoint.

        :param pid: Zero-padded 8-character PID string
        :return: (X, Y) tuple or None if not found
        """
        with arcpy.da.SearchCursor(
            self._parcel_fc, ["SHAPE@"], f"PID LIKE '{pid}'"
        ) as cursor:
            for row in cursor:
                geom = row[0]
                center = geom.trueCentroid
                if geom.contains(center):
                    return center.X, center.Y
                return geom.labelPoint.X, geom.labelPoint.Y
        return None

    def prefetch_polygons(
        self, pids: Iterable[str]
    ) -> dict[str, Optional[arcpy.Geometry]]:
        """
        Bulk-fetch polygon geometry for a collection of PIDs in one pass.

        :param pids: Iterable of zero-padded 8-character PID strings
        :return: Dict mapping each PID to its geometry (or None if not found)
        """
        pid_set = set(pids)
        result: dict[str, Optional[arcpy.Geometry]] = {p: None for p in pid_set}

        if not pid_set:
            return result

        quoted = ", ".join(f"'{p}'" for p in pid_set)
        where = f"PID IN ({quoted})"

        with arcpy.da.SearchCursor(
            self._parcel_fc, ["PID", "SHAPE@"], where
        ) as cursor:
            for pid, geom in cursor:
                if pid in result:
                    result[pid] = geom

        return result

    def prefetch_centroids(
        self, pids: Iterable[str]
    ) -> dict[str, Optional[tuple[float, float]]]:
        """
        Bulk-fetch centroid (X, Y) for a collection of PIDs in one pass.

        :param pids: Iterable of zero-padded 8-character PID strings
        :return: Dict mapping each PID to its centroid tuple (or None)
        """
        pid_set = set(pids)
        result: dict[str, Optional[tuple[float, float]]] = {
            p: None for p in pid_set
        }

        if not pid_set:
            return result

        quoted = ", ".join(f"'{p}'" for p in pid_set)
        where = f"PID IN ({quoted})"

        with arcpy.da.SearchCursor(
            self._parcel_fc, ["PID", "SHAPE@"], where
        ) as cursor:
            for pid, geom in cursor:
                if pid not in result:
                    continue
                center = geom.trueCentroid
                if geom.contains(center):
                    result[pid] = (center.X, center.Y)
                else:
                    lp = geom.labelPoint
                    result[pid] = (lp.X, lp.Y)

        return result


# ---------------------------------------------------------------------------
# Locate result
# ---------------------------------------------------------------------------

@dataclass
class LocateResult:
    """
    Carries the output of a PIDLocator.prepare() call.

    :param geometry_type: ArcPy geometry type string ("POLYGON" or "POINT")
    :param cursor_field: ArcPy cursor token ("SHAPE@" or "SHAPE@XY")
    :param located_df: Records for which a parcel geometry was found
    :param unlocated_df: Records whose PID(s) were not found in the parcel layer
    :param compute_geometry: Callable(pid_value) -> arcpy.Geometry | (X, Y) | None
    """

    geometry_type: str
    cursor_field: str
    located_df: pd.DataFrame
    unlocated_df: pd.DataFrame
    compute_geometry: Callable


# ---------------------------------------------------------------------------
# Locator strategies
# ---------------------------------------------------------------------------

class PIDLocator(ABC):
    """
    Abstract base class for PID-based geometry location strategies.

    Subclasses implement prepare() to inspect a DataFrame of records with
    valid PIDs, look up the relevant parcel geometry, and return a
    LocateResult that splits located vs. unlocated records and provides a
    geometry callable for the FeatureLocator pipeline.

    :param parcel_lookup: ParcelLookup instance wrapping the parcel feature class
    """

    def __init__(self, parcel_lookup: ParcelLookup):
        self._lookup = parcel_lookup

    @abstractmethod
    def prepare(self, records_df: pd.DataFrame, pid_field: str) -> LocateResult:
        """
        Pre-fetch parcel geometry, split records into located/unlocated,
        and return a LocateResult with a compute_geometry callable.

        :param records_df: DataFrame of records that have a non-null PID value
        :param pid_field: Column name of the PID field in records_df
        :return: LocateResult
        """


class PolygonPIDLocator(PIDLocator):
    """
    Locates records as polygons by unioning all parcel polygons for every
    PID in a (potentially comma-separated) PID field value.

    A record is considered located if at least one of its PIDs resolves to a
    parcel polygon. The final geometry is the union of all matched polygons.
    """

    def prepare(self, records_df: pd.DataFrame, pid_field: str) -> LocateResult:
        all_unique_pids: set[str] = set()
        for pid_val in records_df[pid_field].dropna():
            all_unique_pids.update(extract_all_pids(str(pid_val)))

        logger.info(
            f"\tLooking up polygons for {len(all_unique_pids)} unique PIDs..."
        )

        parcel_polygons = self._lookup.prefetch_polygons(all_unique_pids)
        found_count = sum(1 for v in parcel_polygons.values() if v is not None)
        logger.info(
            f"\tFound parcel polygon for {found_count}/{len(all_unique_pids)} PIDs"
        )

        def has_any_polygon(pid_val) -> bool:
            return any(
                parcel_polygons.get(p)
                for p in extract_all_pids(str(pid_val))
            )

        located_mask = records_df[pid_field].apply(has_any_polygon)

        def compute_geometry(pid_value):
            pids = extract_all_pids(pid_value) if pid_value else []
            geometries = [
                parcel_polygons[p] for p in pids if parcel_polygons.get(p)
            ]
            if not geometries:
                return None
            merged = geometries[0]
            for geom in geometries[1:]:
                merged = merged.union(geom)
            return merged

        return LocateResult(
            geometry_type="POLYGON",
            cursor_field="SHAPE@",
            located_df=records_df[located_mask].copy(),
            unlocated_df=records_df[~located_mask].copy(),
            compute_geometry=compute_geometry,
        )


class PointPIDLocator(PIDLocator):
    """
    Locates records as points at the centroid of the primary (first) PID's
    parcel polygon.

    Only the first PID in a comma-separated PID value is used. A record is
    considered located if that PID resolves to a parcel centroid.
    """

    def prepare(self, records_df: pd.DataFrame, pid_field: str) -> LocateResult:
        primary_pids = records_df[pid_field].apply(extract_primary_pid)
        unique_primary_pids = primary_pids.dropna().unique().tolist()

        logger.info(
            f"\tLooking up centroids for {len(unique_primary_pids)} unique PIDs..."
        )

        centroids = self._lookup.prefetch_centroids(unique_primary_pids)
        found_count = sum(1 for v in centroids.values() if v is not None)
        logger.info(
            f"\tFound parcel centroid for {found_count}/{len(unique_primary_pids)}"
            " PIDs"
        )

        located_mask = primary_pids.apply(lambda p: bool(centroids.get(p)))

        def compute_geometry(pid_value):
            return centroids.get(extract_primary_pid(pid_value))

        return LocateResult(
            geometry_type="POINT",
            cursor_field="SHAPE@XY",
            located_df=records_df[located_mask].copy(),
            unlocated_df=records_df[~located_mask].copy(),
            compute_geometry=compute_geometry,
        )


# ---------------------------------------------------------------------------
# Feature locator pipeline
# ---------------------------------------------------------------------------

class FeatureLocator:
    """
    Orchestrates the full PID-to-feature pipeline.

    Creates a temporary feature class in the scratch workspace, appends
    attribute data from a DataFrame (via CSV export), then populates geometry
    using the strategy provided by a PIDLocator.

    Preconditions:
    - arcpy.env.overwriteOutput should be True in the calling script.
    - The scratch workspace must already exist.

    :param scratch_workspace: Path to the scratch file geodatabase
    :param spatial_reference: arcpy.SpatialReference for output features
    :param exports_dir: Directory for intermediate CSV exports
    :param locator: PIDLocator instance (PolygonPIDLocator or PointPIDLocator)
    """

    def __init__(
        self,
        scratch_workspace: str,
        spatial_reference: arcpy.SpatialReference,
        exports_dir: str,
        locator: PIDLocator,
    ):
        self._scratch_workspace = scratch_workspace
        self._spatial_reference = spatial_reference
        self._exports_dir = exports_dir
        self._locator = locator

    def locate(
        self,
        records_df: pd.DataFrame,
        target_feature: str,
        pid_field: str,
        sde_pid_field: str,
        field_map_dict: Optional[dict] = None,
    ) -> tuple[str, pd.DataFrame]:
        """
        Run the full locate pipeline and return the temp feature class path and
        a DataFrame of records that could not be located.

        Steps:
        1. Delegate to the locator strategy to pre-fetch geometry and split
           located/unlocated records.
        2. Export located records to CSV (arcpy compatibility).
        3. Create a temp feature class using the target SDE feature as a
           schema template.
        4. Append CSV records into the temp feature class.
        5. Populate geometry via UpdateCursor using the strategy's
           compute_geometry callable.

        :param records_df: DataFrame of records that have a non-null PID value
        :param target_feature: Path to the target SDE feature class (schema
            template)
        :param pid_field: Name of the PID column in records_df
        :param sde_pid_field: Name of the PID field in the SDE feature class
            (used as the UpdateCursor key)
        :param field_map_dict: Optional {sde_field: source_column} dict for
            explicit field mapping during the CSV Append. Required when DW
            column names differ from SDE field names.
        :return: (located_temp_feature_path, unlocated_df)
        """
        feature_name = os.path.basename(target_feature).replace("SDEADM.", "")
        temp_feature_name = f"{feature_name}_pid_located"

        logger.info(
            f"Generating PID features for '{feature_name}' "
            f"({len(records_df)} records) "
            f"[mode: {type(self._locator).__name__}]..."
        )

        result = self._locator.prepare(records_df, pid_field)

        logger.info(f"\tRecords locatable: {len(result.located_df)}")
        logger.info(f"\tRecords unlocated: {len(result.unlocated_df)}")

        # Export located records to CSV for arcpy compatibility
        os.makedirs(self._exports_dir, exist_ok=True)
        export_csv = os.path.join(
            self._exports_dir, f"{feature_name}_pid_records.csv"
        )
        result.located_df.to_csv(export_csv, index=False)

        # Create temp feature class using target as schema template
        temp_feature = arcpy.CreateFeatureclass_management(
            out_path=self._scratch_workspace,
            out_name=temp_feature_name,
            geometry_type=result.geometry_type,
            template=target_feature,
            spatial_reference=self._spatial_reference,
        )[0]

        # Append CSV records into the temp feature (geometry populated below).
        # Use explicit field mapping when DW column names differ from SDE names.
        if field_map_dict:
            field_mapping = build_field_mapping(
                export_csv, temp_feature, field_map_dict
            )
            arcpy.Append_management(
                inputs=export_csv,
                target=temp_feature,
                schema_type="NO_TEST",
                field_mapping=field_mapping,
            )
        else:
            arcpy.Append_management(
                inputs=export_csv,
                target=temp_feature,
                schema_type="NO_TEST",
            )

        logger.info(
            f"\tAppended {arcpy.GetCount_management(temp_feature)[0]}"
            " records to temp feature"
        )

        # Populate geometry for each row; delete rows with no geometry found
        with arcpy.da.UpdateCursor(
            temp_feature, [sde_pid_field, result.cursor_field]
        ) as cursor:
            for row in cursor:
                geom = result.compute_geometry(row[0])
                if geom is not None:
                    row[1] = geom
                    cursor.updateRow(row)
                else:
                    cursor.deleteRow()

        logger.info(
            f"\tLocated: {arcpy.GetCount_management(temp_feature)[0]} features"
        )

        return temp_feature, result.unlocated_df


# ---------------------------------------------------------------------------
# Geometry mode registry
# ---------------------------------------------------------------------------

GEOMETRY_MODES: dict[str, type[PIDLocator]] = {
    "POLYGON": PolygonPIDLocator,
    "POINT": PointPIDLocator,
}
