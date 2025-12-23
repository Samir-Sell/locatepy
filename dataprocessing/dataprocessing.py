import json
import sqlite3
from typing import Union
from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.wkb import dumps as wkb_dumps
from shapely import STRtree
from shapely.prepared import prep




SQL_LITE_DATA_PATH = r".\pylocatator\data\data.db"
SCHEMA_SQL_LITE_DATA_PATH = r".\dataprocessing\schema.sql"
COUNTRIES_GEOJSON_PATH = r"C:\Users\ssellars\Downloads\geoBoundariesCGAZ_ADM0.geojson"
STATES_GEOJSON_PATH = r"C:\Users\ssellars\Downloads\geoBoundariesCGAZ_ADM1.geojson"
MUNICIPAL_GEOJSON_PATH = r"C:\Users\ssellars\Downloads\geoBoundariesCGAZ_ADM2.geojson"


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
        geom = shape(feature["geometry"])
        minx, miny, maxx, maxy = geom.bounds

        name = props.get("shapeName")
        code = props.get("shapeGroup")
        srid = 4326  # assuming WGS84

        # Insert into countries table
        cur.execute("""
            INSERT INTO countries (name, code, srid, geom_wkb, minx, miny, maxx, maxy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, code, srid, sqlite3.Binary(wkb_dumps(ob=geom, srid=4326)), minx, miny, maxx, maxy))

        # Insert into R-tree
        country_id = cur.lastrowid
        cur.execute("""
            INSERT INTO countries_rtree (id, minx, maxx, miny, maxy)
            VALUES (?, ?, ?, ?, ?)
        """, (country_id, minx, maxx, miny, maxy))
        
        con.commit()


def ingest_states(con: sqlite3.Connection, geojson_path: str):
    with open(geojson_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    cur = con.cursor()
    for feature in data["features"]:
        props = feature.get("properties", {}) or {}
        geom = shape(feature["geometry"])  # Polygon or MultiPolygon

        minx, miny, maxx, maxy = geom.bounds

        name = props.get("shapeName")
        code = props.get("shapeID")
        country_code: str = props.get("shapeGroup") # type: ignore

        country_id = lookup_country_id_by_code(con, country_code)

        # Insert into states
        cur.execute("""
            INSERT INTO states (country_id, code, name, srid, geom_wkb, minx, miny, maxx, maxy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (country_id, code, name, 4326, sqlite3.Binary(wkb_dumps(ob=geom, srid=4326)),
              float(minx), float(miny), float(maxx), float(maxy)))

        state_id = cur.lastrowid

        # Insert into states_rtree
        cur.execute("""
            INSERT INTO states_rtree (id, minx, maxx, miny, maxy)
            VALUES (?, ?, ?, ?, ?)
        """, (state_id, float(minx), float(maxx), float(miny), float(maxy)))

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
            cand_idxs = list(tree.query(geom))  # Shapely 2.x returns indices

            best_idx = None
            best_ratio = -1.0

            # Prefer covers (boundary-inclusive), then intersects, by max intersection ratio
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
                nearest_idx = tree.nearest(geom)  # Shapely 2.x returns an index
                best_idx = nearest_idx

            found_state_geo = state_polygons_list[best_idx]


            # Map properties (tweak these to your dataset)
            name = props.get("shapeName")
            code = props.get("shapeId")
            country_code = props.get("shapeGroup")

            # Parent lookups (attribute-based)
            state_id = lookup_table[found_state_geo] 
            country_id = lookup_country_id_by_code(con, country_code)

            # if state_id is None:
            #     missing_state += 1
            # if country_id is None:
            #     missing_country += 1

            if name is None:
                name = "UNKNOWN NAME"

            # Insert into municipalities
            cur.execute("""
                INSERT INTO municipalities (state_id, country_id, code, name, srid, geom_wkb, minx, miny, maxx, maxy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (state_id, country_id, code, name, 4326, sqlite3.Binary(wkb_dumps(geom)),
                    float(minx), float(miny), float(maxx), float(maxy)))

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




# def ingest_municipalities(con: sqlite3.Connection, geojson_path: str):




con = connect_to_sql_lite_db(SQL_LITE_DATA_PATH)
run_schema(con, SCHEMA_SQL_LITE_DATA_PATH)
ingest_countries(con, COUNTRIES_GEOJSON_PATH)
ingest_states(con, STATES_GEOJSON_PATH)
ingest_municipalities(con, STATES_GEOJSON_PATH, MUNICIPAL_GEOJSON_PATH)
