-- Countries
CREATE TABLE IF NOT EXISTS countries (
  id            INTEGER PRIMARY KEY,
  code          TEXT,
  name          TEXT NOT NULL
);

-- States
CREATE TABLE IF NOT EXISTS states (
  id            INTEGER PRIMARY KEY,
  code          TEXT,
  name          TEXT NOT NULL
);

-- Municipalities
CREATE TABLE IF NOT EXISTS municipalities (
  id            INTEGER PRIMARY KEY,
  state_id      INTEGER NOT NULL REFERENCES states(id),
  country_id    INTEGER NOT NULL REFERENCES countries(id),
  code          TEXT,
  name          TEXT NOT NULL,
  geom_wkb      BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_munis_state    ON municipalities(state_id);
CREATE INDEX IF NOT EXISTS idx_munis_country  ON municipalities(country_id);
CREATE VIRTUAL TABLE IF NOT EXISTS municipalities_rtree USING rtree(
  id, minx, maxx, miny, maxy
);