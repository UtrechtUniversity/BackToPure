"""
Test Suite for the get_contributors_details Function

This script contains tests for the get_contributors_details function, which is
responsible for processing a list of contributors, identifying whether they are
internal or external to the system, and creating records for external contributors.

Each test function is designed to cover a specific scenario, ensuring that the
get_contributors_details function handles various cases correctly, including:
- Processing internal contributors
- Handling external contributors
- Dealing with empty or invalid input data

Requirements:
- pytest
- Mocking capabilities for external dependencies like pure_persons.find_person

Usage:
Run the tests using the pytest command in the root directory of the project:
$ pytest path/to/test_script.py

Author: David Grote Beverborg
Date: 04-04-2024
Version: 0.1
"""
from pathlib import Path
import pytest
import sys
# Add src directory to sys.path to find pure_persons
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))
from src.pure_datasets import get_contributors_details, create_external_person, split_name


@pytest.fixture
def internal_contributor():
    return {
        "name": "John Doe",
        "person_ids": [{"id": "some_id", "value": "some_value"}]
    }

@pytest.fixture
def external_contributor():
    return {
        "name": "Jane Smith",
        "person_ids": [{"id": "other_id", "value": "other_value"}]
    }

def test_with_internal_contributors(internal_contributor):
    """Test get_contributors_details with internal contributors."""
    contributors = [internal_contributor]
    # Mock response from pure_persons.find_person as if it found the person
    result = get_contributors_details(contributors)
    assert internal_contributor["name"] in result
    # Additional assertions based on expected result

def test_with_external_contributors(external_contributor):
    """Test get_contributors_details with external contributors."""
    contributors = [external_contributor]
    # Mock response from pure_persons.find_person as if it did not find the person
    result = get_contributors_details(contributors)
    assert external_contributor["name"] in result
    # Check if the external person is created correctly

def test_with_no_contributors():
    """Test get_contributors_details with no contributors."""
    result = get_contributors_details([])
    assert result is None

def test_with_invalid_contributor_format():
    """Test get_contributors_details with invalid contributor format."""
    contributors = [{"invalid": "data"}]  # Incorrectly formatted data
    with pytest.raises(KeyError):  # Expecting a KeyError due to missing 'name' and 'person_ids' keys
        get_contributors_details(contributors)
