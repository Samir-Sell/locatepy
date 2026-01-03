"""
LocatePy Database Builder
=========================

This script builds the LocatePy SQLite database from GeoJSON boundary files.

It ingests:
- Countries (ADM0)
- States / Districts (ADM1)
- Municipalities (ADM2)

Key characteristics:
--------------------
- Uses Shapely for geometry handling
- Uses STRtree for fast state-to-municipality matching
- Stores municipal geometries as zlib-compressed WKB
- Populates an SQLite R-Tree for spatial queries

This script is intended to be run once to generate the database used by LocatePy.
"""

import json
import sqlite3
import zlib
from typing import Union

from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.prepared import prep
from shapely.strtree import STRtree
from shapely.wkb import dumps as wkb_dumps
from shapely import simplify


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SQLITE_DB_PATH = r".\locatepy\data\world-admin-boundstest.db"
SCHEMA_SQL_PATH = r".\dataprocessing\schema.sql"

COUNTRIES_GEOJSON_PATH = r".\dataprocessing\geoBoundariesCGAZ_ADM0.geojson"
STATES_GEOJSON_PATH = r".\dataprocessing\geoBoundariesCGAZ_ADM1.geojson"
MUNICIPAL_GEOJSON_PATH = r".\dataprocessing\geoBoundariesCGAZ_ADM2.geojson"


# ---------------------------------------------------------------------------
# Database Helpers
# ---------------------------------------------------------------------------

def connect_to_sqlite(db_path: str = SQLITE_DB_PATH) -> sqlite3.Connection:
    """Create and return a SQLite connection."""
    return sqlite3.connect(db_path)


def run_schema(con: sqlite3.Connection, schema_sql_path: str) -> None:
    """Execute the database schema SQL."""
    with open(schema_sql_path, "r", encoding="utf-8") as f:
        con.executescript(f.read())
    con.commit()


def lookup_country_id(con: sqlite3.Connection, country_code: str) -> Union[int, None]:
    """Return country ID for a given country code."""
    row = con.execute(
        "SELECT id FROM countries WHERE code = ?",
        (country_code,),
    ).fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Ingest Functions
# ---------------------------------------------------------------------------

def ingest_countries(con: sqlite3.Connection, geojson_path: str) -> None:
    """Insert country records (ADM0) from a GeoJSON file."""
    with open(geojson_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    cur = con.cursor()
    for feature in data["features"]:
        props = feature.get("properties", {}) or {}

        cur.execute(
            """
            INSERT INTO countries (name, code)
            VALUES (?, ?)
            """,
            (
                props.get("shapeName"),
                props.get("shapeGroup"),
            ),
        )

    con.commit()


def ingest_states(con: sqlite3.Connection, geojson_path: str) -> None:
    """Insert state/district records (ADM1) from a GeoJSON file."""
    with open(geojson_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    cur = con.cursor()
    for feature in data["features"]:
        props = feature.get("properties", {}) or {}

        cur.execute(
            """
            INSERT INTO states (code, name)
            VALUES (?, ?)
            """,
            (
                props.get("shapeID"),
                props.get("shapeName"),
            ),
        )

    con.commit()


def ingest_municipalities(
    con: sqlite3.Connection,
    state_geojson_path: str,
    municipal_geojson_path: str,
) -> None:
    """
    Insert municipal records (ADM2) and build R-Tree index.

    Municipalities are assigned to states by maximum intersection area,
    with a nearest-neighbor fallback.
    """

    # --- Load state geometries ---
    with open(state_geojson_path, "r", encoding="utf-8") as f:
        state_data = json.load(f)

    state_geoms: list[Union[Polygon, MultiPolygon]] = []
    state_lookup: dict[Union[Polygon, MultiPolygon], str] = {}

    print("Processing state geometries...")
    for feature in state_data["features"]:
        geom = shape(feature["geometry"])
        state_id = feature.get("properties", {}).get("shapeID")

        state_geoms.append(geom)
        state_lookup[geom] = state_id

    tree = STRtree(state_geoms)
    prepared_states = [prep(g) for g in state_geoms]

    # --- Load municipal geometries ---
    with open(municipal_geojson_path, "r", encoding="utf-8") as f:
        muni_data = json.load(f)

    cur = con.cursor()
    failed = []
    total = len(muni_data["features"])

    print(f"Processing {total} municipalities...")

    for idx, feature in enumerate(muni_data["features"], start=1):
        try:
            props = feature.get("properties", {}) or {}
            geom = shape(feature["geometry"])

            minx, miny, maxx, maxy = geom.bounds

            # Find candidate states
            candidates = tree.query(geom)

            best_state_idx = None
            best_ratio = -1.0

            for cand_idx in candidates:
                pg = prepared_states[cand_idx]
                sg = state_geoms[cand_idx]

                if pg.intersects(geom):
                    area_ratio = (
                        sg.intersection(geom).area / geom.area
                        if geom.area > 0
                        else 0.0
                    )
                    if area_ratio > best_ratio:
                        best_ratio = area_ratio
                        best_state_idx = cand_idx

            if best_state_idx is None:
                best_state_idx = tree.nearest(geom)

            state_id = state_lookup[state_geoms[best_state_idx]]
            country_id = lookup_country_id(con, props.get("shapeGroup"))

            name = props.get("shapeName") or "UNKNOWN NAME"
            code = props.get("shapeId")

            geom_wkb = sqlite3.Binary(
                zlib.compress(
                    wkb_dumps(simplify(geom, 0.001))
                )
            )

            cur.execute(
                """
                INSERT INTO municipalities
                (state_id, country_id, code, name, geom_wkb)
                VALUES (?, ?, ?, ?, ?)
                """,
                (state_id, country_id, code, name, geom_wkb),
            )

            muni_id = cur.lastrowid

            cur.execute(
                """
                INSERT INTO municipalities_rtree
                (id, minx, maxx, miny, maxy)
                VALUES (?, ?, ?, ?, ?)
                """,
                (muni_id, minx, maxx, miny, maxy),
            )

            if idx % 50 == 0:
                print(f"{idx}/{total}")

        except Exception as exc:
            failed.append(
                {
                    "properties": feature.get("properties", {}),
                    "error": str(exc),
                }
            )

    con.commit()

    if failed:
        print(f"Failed to ingest {len(failed)} municipalities")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    con = connect_to_sqlite(SQLITE_DB_PATH)
    run_schema(con, SCHEMA_SQL_PATH)
    ingest_countries(con, COUNTRIES_GEOJSON_PATH)
    ingest_states(con, STATES_GEOJSON_PATH)
    ingest_municipalities(con, STATES_GEOJSON_PATH, MUNICIPAL_GEOJSON_PATH)
