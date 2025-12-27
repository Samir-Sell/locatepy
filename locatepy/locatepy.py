
"""
LocatePy: Reverse Geolocation Utility
======================================

This module provides functionality to perform reverse geolocation queries
against a prebuilt SQLite database of world administrative boundaries.

Features:
---------
- Identify the administrative hierarchy (Country, District, Municipal) for a given latitude/longitude.
- Uses spatial indexing (R-Tree) for efficient lookup.
- Geometry operations powered by Shapely.
- Compressed WKB geometries for storage efficiency.
"""

import sqlite3
import os
from zlib import decompress
from dataclasses import dataclass
from shapely.geometry import Point
from shapely.wkb import loads as wkb_loads

@dataclass
class LocateResult:
    """The return from a locate search representing three levels of admin boundaries"""
    country: str # ADM0
    district: str # ADM1
    municipal: str # ADM2

class LocatePy:
    """Perform reverse geolocation queries to return admin boundaries"""
    def __init__(self, world_admin_bounds_db: str = r"locatepy\data\world-admin-bounds.db"):
        if os.path.exists(world_admin_bounds_db) is False:
            raise FileNotFoundError("World Admin Database Missing")
        self._world_con = sqlite3.connect(world_admin_bounds_db)

    def locate(self, lat: float, lon: float):
        """Reverse geolocate a latitude and longitude"""
        return self._find_world_admin(self._world_con, lat, lon)

    def _find_world_admin(self, con: sqlite3.Connection, lat: float, lon: float) -> LocateResult:
        sql = """
        SELECT m.name AS municipal_name, m.geom_wkb AS geom, s.name AS district_name, c.name AS country_name
        FROM municipalities_rtree r
        JOIN municipalities m ON m.id = r.id
        JOIN states s ON s.code = m.state_id
        JOIN countries c ON c.id = m.country_id
        WHERE r.minx <= ? AND r.maxx >= ? AND r.miny <= ? AND r.maxy >= ?
        """
        pt = Point(lon, lat)
        rows = con.execute(sql, (lon, lon, lat, lat)).fetchall()
        for row in rows:
            geom = wkb_loads(decompress(row[1]))
            if geom.covers(pt):
                return LocateResult(
                    country=row[3],
                    district=row[2],
                    municipal=row[0]
                )
        return LocateResult("UNKNOWN", "UNKNOWN", "UNKNOWN")
