CREATE TABLE IF NOT EXISTS addresses (
  id            INTEGER PRIMARY KEY,
  address       TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS addresses_rtree USING rtree(
  id, minx, maxx, miny, maxy
);
CREATE INDEX IF NOT EXISTS idx_address_ids ON addresses(id);
