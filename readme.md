# LocatePy

**LocatePy** is a lightweight, fast reverse-geolocation utility for Python.  
It maps latitude/longitude coordinates to administrative boundaries using a **prebuilt SQLite database** with **R-Tree spatial indexing** and **compressed WKB geometries** for exact geometry verification.

This project is designed to be:

- **Fast** — minimal dependencies, only Shapely
- **Lightweight** — SQLite + compressed geometries
- **Offline-first** — no API calls, no internet required

## Features

- Reverse geolocation from `(lat, lon)` → **Country / District / Municipal**
- Spatial lookup via **SQLite R-Tree**
- Geometry checks powered by **Shapely**
- **Compressed WKB** for efficient storage
- Simple API
- Data for geolocation provided by [geoboundaries.org](https://www.geoboundaries.org/)

## Usage

### Basic Example

```python
from locatepy.locatepy import LocatePy

locator = LocatePy()
result = locator.locate(lat=43.6532, lon=-79.3832)

print(result.country) # Canada
print(result.district) # Ontario
print(result.municipal) # Toronto
```

### Returned Object

`locate()` returns a `LocateResult` dataclass:

```python
LocateResult(
    country: str,    # ADM0
    district: str,   # ADM1
    municipal: str   # ADM2
)
```

If no match is found:

```python
LocateResult("UNKNOWN", "UNKNOWN", "UNKNOWN")
```

## Installation

```
pip install locatepy
```

### Dependencies

LocatePy requires:

- Python 3.9+
- `shapely`
- Ships with a prebuilt SQLite database

### Geometry Storage

- Municipal geometries stored as **zlib-compressed WKB**
- Bounding boxes stored in the R-Tree (`minx`, `maxx`, `miny`, `maxy`)

## How It Works

1. A `Point(lon, lat)` is created using Shapely
2. The SQLite **R-Tree** is queried for candidate municipalities
3. Candidate geometries are:
   - Decompressed from zlib
   - Loaded from WKB
4. A precise shapely covers test determines containment
5. The matching admin hierarchy is returned

This approach avoids expensive full-table scans and keeps memory usage low.

## Performance Notes

- R-Tree bounding box filtering reduces geometry checks dramatically
- Geometry decompression occurs **only** for candidate matches
- No internet connection required

## Limitations

- Resolution and accuracy depend entirely on the source boundary data provided by [geoboundaries.org](https://www.geoboundaries.org/)
- Does not currently support:
  - Reverse geocoding to street/address level
  - Multi-match results
  - Assumes WGS84


## License

MIT License


**LocatePy — simple, fast, offline reverse geolocation.**
