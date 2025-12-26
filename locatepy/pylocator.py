
from zlib import decompress
from shapely.geometry import Point
from shapely.wkb import loads as wkb_loads
import sqlite3


def find_admin(con: sqlite3.Connection, lat: float, lon: float, pt: Point):
    sql = """
    SELECT m.name AS municipal_name, m.geom_wkb AS geom, s.name AS district_name, c.name AS country_name
    FROM municipalities_rtree r
    JOIN municipalities m ON m.id = r.id
    JOIN states s ON s.code = m.state_id
    JOIN countries c ON c.id = m.country_id
    WHERE r.minx <= ? AND r.maxx >= ? AND r.miny <= ? AND r.maxy >= ?
    """
    rows = con.execute(sql, (lon, lon, lat, lat)).fetchall()
    for row in rows:
        geom = wkb_loads(decompress(row[1]))
        if geom.covers(pt):
            return {"municipal_name": row[0], "district_name": row[2], "country_name": row[3]}
    return None

def geocode_point(lat: float, lon: float):
    con: sqlite3.Connection = sqlite3.connect(r"locatepy\data\compressed.db")
    pt = Point(lon, lat)
    location = find_admin(con, lat, lon, pt)
    if location is None:
        return {"municipal_name": "UNKNOWN", "district_name": "UNKNOWN", "country_name": "UNKNOWN"}
    else:
        return location
    
print(geocode_point(50.638793, 5.563240))