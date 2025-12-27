"""Simple tests to ensure locatepy is functioning correctly."""

import pytest
from locatepy.locatepy import LocatePy, LocateResult

def test_no_valid_database():
    """Should raise FileNotFoundError if DB path does not exist."""
    missing_db = "missing.db"
    with pytest.raises(FileNotFoundError):
        LocatePy(str(missing_db))


def test_ottawa_locate_returns_result():
    """Should return a LocateResult for a valid point using the packaged DB."""
    locator = LocatePy()
    result = locator.locate(45.4215, -75.6972)
    assert isinstance(result, LocateResult)
    assert hasattr(result, "country")
    assert hasattr(result, "district")
    assert hasattr(result, "municipal")
    assert result.country == "Canada"
    assert result.district == "Ontario"
    assert result.municipal == "Ottawa"

def test_rhone_returns_result():
    """Should return a LocateResult for a valid point using the packaged DB."""
    locator = LocatePy()
    result = locator.locate(45.762712, 4.859840)
    assert result.country == "France"
    assert result.district == "Auvergne-Rhône-Alpes"
    assert result.municipal == "Rhône"

def test_ocean_returns_result():
    """Should return a LocateResult for a valid point using the packaged DB."""
    locator = LocatePy()
    result = locator.locate(6.200442, -29.694501)
    assert result.country == "UNKNOWN"
    assert result.district == "UNKNOWN"
    assert result.municipal == "UNKNOWN"

def test_ontario_lake_superior_returns_result():
    """Should return a LocateResult for a valid point using the packaged DB."""
    locator = LocatePy()
    result = locator.locate(48.623859, -87.26383)
    assert result.country == "UNKNOWN"
    assert result.district == "UNKNOWN"
    assert result.municipal == "UNKNOWN"
