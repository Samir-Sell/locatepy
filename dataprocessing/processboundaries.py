import json
import sqlite3
from typing import Union
from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.wkb import dumps as wkb_dumps
from shapely import STRtree, simplify
from shapely.prepared import prep
import zlib


SQL_LITE_DATA_PATH = r".\locatepy\data\world-admin-bounds.db"
SCHEMA_SQL_LITE_DATA_PATH = r"schema.sql"
COUNTRIES_GEOJSON_PATH = r"geoBoundariesCGAZ_ADM0.geojson"
STATES_GEOJSON_PATH = r"geoBoundariesCGAZ_ADM1.geojson"
MUNICIPAL_GEOJSON_PATH = r"geoBoundariesCGAZ_ADM2.geojson"


def connect_to_sql_lite_db(db_path: str = SQL_LITE_DATA_PATH):
    con = sqlite3.connect(db_path)
    return con

def run_schema(con: sqlite3.Connection, schema_sql_path: str):
    with open(schema_sql_path, "r", encoding="utf-8") as f:
        cur = con.cursor()
        cur.executescript(f.read())
        con.commit()

def lookup_country_id_by_code(con: sqlite3.Connection, country_code: str) -> Union[int,None]:
    row = con.execute("SELECT id FROM countries WHERE code = ?", (country_code,)).fetchone()
    return row[0]


def ingest_countries(con: sqlite3.Connection, geojson_path: str):
    with open(geojson_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    cur = con.cursor()
    for feature in data["features"]:
        props = feature.get("properties", {})

        name = props.get("shapeName")
        code = props.get("shapeGroup")

        cur.execute("""
            INSERT INTO countries (name, code)
            VALUES (?, ?)
        """, (name, code))
        con.commit()


def ingest_states(con: sqlite3.Connection, geojson_path: str):
    with open(geojson_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    cur = con.cursor()
    for feature in data["features"]:
        props = feature.get("properties", {}) or {}

        name = props.get("shapeName")
        code = props.get("shapeID")

        cur.execute("""
            INSERT INTO states (code, name)
            VALUES (?, ?)
        """, (code, name))

    con.commit()


def ingest_municipalities(con, state_geojson_path: str, municipal_geojson_path:str):
    
    with open(state_geojson_path, "r", encoding="utf-8") as f:
        state_data = json.load(f)

    lookup_table: dict[Union[Polygon,MultiPolygon], str] = {}
    state_polygons_list: list[Union[MultiPolygon, Polygon]] = []

    print("Processing state polygons")
    for feature in state_data["features"]:
        props = feature.get("properties", {}) or {}
        geom : Union[Polygon, MultiPolygon] = shape(feature["geometry"])  # type: ignore # Polygon or MultiPolygon
        lookup_table[geom] = props.get("shapeID") # type: ignore
        state_polygons_list.append(geom)
    
    print("Creating STR treee")
    tree = STRtree(state_polygons_list)
    state_prepared = [prep(g) for g in state_polygons_list]

    print("Processing municipal polygons")
    with open(municipal_geojson_path, "r", encoding="utf-8") as f:
        muni_data = json.load(f)
    cur = con.cursor()
    progress = 0

    print(f"Length To Process: {len(muni_data['features'])}")
    failed_munis = []
    for feature in muni_data["features"]:
        try:
            props = feature.get("properties", {}) or {}
            geom = shape(feature["geometry"])  # Polygon or MultiPolygon
            minx, miny, maxx, maxy = geom.bounds

            
            # Get candidate state indices that overlap the muni bbox/geom
            cand_idxs = list(tree.query(geom))

            best_idx = None
            best_ratio = -1.0

            for idx in cand_idxs:
                pg = state_prepared[idx]
                sg = state_polygons_list[idx]

                if pg.covers(geom) or pg.intersects(geom):
                    inter_area = sg.intersection(geom).area
                    ratio = inter_area / geom.area if geom.area > 0 else 0.0
                    if ratio > best_ratio:
                        best_ratio = ratio
                        best_idx = idx

            # Final fallback: only if no candidate overlaps, pick nearest
            if best_idx is None:
                nearest_idx = tree.nearest(geom)
                best_idx = nearest_idx

            found_state_geo = state_polygons_list[best_idx]


            name = props.get("shapeName")
            code = props.get("shapeId")
            country_code = props.get("shapeGroup")

            state_id = lookup_table[found_state_geo] 
            country_id = lookup_country_id_by_code(con, country_code)

            if name is None:
                name = "UNKNOWN NAME"

            # Insert into municipalities
            cur.execute("""
                INSERT INTO municipalities (state_id, country_id, code, name, geom_wkb)
                VALUES (?, ?, ?, ?, ?)
            """, (state_id, country_id, code, name, sqlite3.Binary(zlib.compress(wkb_dumps(simplify(geom, 0.001))))))
            muni_id = cur.lastrowid

            # Insert bbox into R-tree
            cur.execute("""
                INSERT INTO municipalities_rtree (id, minx, maxx, miny, maxy)
                VALUES (?, ?, ?, ?, ?)
            """, (muni_id, float(minx), float(maxx), float(miny), float(maxy)))
            progress = progress + 1
            if progress % 50 == 0:
                print(progress)
        except Exception as e:
            failed_items = {"feature": feature.get("properties", {}) or {}, "error": e}
            failed_munis.append(failed_items)
            print(failed_items)


    con.commit()

con = connect_to_sql_lite_db(SQL_LITE_DATA_PATH)
run_schema(con, SCHEMA_SQL_LITE_DATA_PATH)
ingest_countries(con, COUNTRIES_GEOJSON_PATH)
ingest_states(con, STATES_GEOJSON_PATH)
ingest_municipalities(con, STATES_GEOJSON_PATH, MUNICIPAL_GEOJSON_PATH)
