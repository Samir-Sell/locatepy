-- Countries
CREATE TABLE IF NOT EXISTS countries (
  id            INTEGER PRIMARY KEY,
  code          TEXT,
  name          TEXT NOT NULL,
  srid          INTEGER NOT NULL,
  geom_wkb      BLOB NOT NULL,
  minx          REAL NOT NULL, miny REAL NOT NULL,
  maxx          REAL NOT NULL, maxy REAL NOT NULL
);
CREATE VIRTUAL TABLE IF NOT EXISTS countries_rtree USING rtree(
  id, minx, maxx, miny, maxy
);

-- States/Provinces
CREATE TABLE IF NOT EXISTS states (
  id            INTEGER PRIMARY KEY,
  country_id    INTEGER NOT NULL REFERENCES countries(id),
  code          TEXT,
  name          TEXT NOT NULL,
  srid          INTEGER NOT NULL,
  geom_wkb      BLOB NOT NULL,
  minx          REAL NOT NULL, miny REAL NOT NULL,
  maxx          REAL NOT NULL, maxy REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_states_country ON states(country_id);
CREATE VIRTUAL TABLE IF NOT EXISTS states_rtree USING rtree(
  id, minx, maxx, miny, maxy
);

-- Municipalities
CREATE TABLE IF NOT EXISTS municipalities (
  id            INTEGER PRIMARY KEY,
  state_id      INTEGER NOT NULL REFERENCES states(id),
  country_id    INTEGER NOT NULL REFERENCES countries(id),
  code          TEXT,
  name          TEXT NOT NULL,
  srid          INTEGER NOT NULL,
  geom_wkb      BLOB NOT NULL,
  minx          REAL NOT NULL, miny REAL NOT NULL,
  maxx          REAL NOT NULL, maxy REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_munis_state    ON municipalities(state_id);
CREATE INDEX IF NOT EXISTS idx_munis_country  ON municipalities(country_id);
CREATE VIRTUAL TABLE IF NOT EXISTS municipalities_rtree USING rtree(
  id, minx, maxx, miny, maxy
);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
