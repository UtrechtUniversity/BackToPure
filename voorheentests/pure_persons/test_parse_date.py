import pytest
from datetime import datetime

import sys
from pathlib import Path

# Add src directory to sys.path to find pure_persons
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from pure_persons import parse_date  # Import from pure_persons



# Testing with valid date strings
@pytest.mark.parametrize("test_input,expected", [
    ("2020-01-01", datetime(2020, 1, 1)),
    ("2020-12-31", datetime(2020, 12, 31)),
    # Add more test cases for different formats if applicable
])
def test_parse_date_valid(test_input, expected):
    assert parse_date(test_input) == expected

# Testing with invalid date strings
@pytest.mark.parametrize("test_input", [
    "2020-02-30",  # Invalid date
    "not-a-date",   # Completely wrong format
    "20200101",     # Missing separators
    # Add more invalid cases
])
def test_parse_date_invalid(test_input):
    assert parse_date(test_input) is None

# Testing with None or empty string
@pytest.mark.parametrize("test_input", [None, ""])
def test_parse_date_none_or_empty(test_input):
    assert parse_date(test_input) is None

# Add more tests if your function handles more scenarios
