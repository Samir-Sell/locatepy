import sqlite3
import csv


SQL_LITE_DATA_PATH = r".\locatepy\data\canadian-addresses.db"
SCHEMA_SQL_LITE_DATA_PATH = r".\dataprocessing\canadian_addresses.sql"
ONTARIO_ADDRESSES_CSV = r"C:\Users\ssellars\Downloads\ODA_ON_v1\ODA_ON_v1.csv"


def connect_to_sql_lite_db(db_path: str = SQL_LITE_DATA_PATH):
    con = sqlite3.connect(db_path)
    return con

def run_schema(con: sqlite3.Connection, schema_sql_path: str):
    with open(schema_sql_path, "r", encoding="utf-8") as f:
        cur = con.cursor()
        cur.executescript(f.read())
        con.commit()

con = connect_to_sql_lite_db(SQL_LITE_DATA_PATH)
run_schema(con, SCHEMA_SQL_LITE_DATA_PATH)

with open(ONTARIO_ADDRESSES_CSV, mode='r') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    next(csv_reader) 
    con = connect_to_sql_lite_db(SQL_LITE_DATA_PATH)
    cur = con.cursor()
    progress = 0
    print(csv_reader.line_num)
    for row in csv_reader:
        x = row[0]
        y = row[1]
        address = row[13]
        # Insert into municipalities
        cur.execute("""
            INSERT INTO addresses (address)
            VALUES (?)
        """, (address,))
        address_id = cur.lastrowid

        # Insert bbox into R-tree
        cur.execute("""
            INSERT INTO addresses_rtree (id, minx, maxx, miny, maxy)
            VALUES (?, ?, ?, ?, ?)
        """, (address_id, float(x), float(x), float(y), float(y)))
        progress = progress + 1
        if progress % 500000 == 0:
            con.commit()
            print(progress)
    con.commit()
        