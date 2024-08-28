import pytest
from pathlib import Path
import sys
# Add src directory to sys.path to find pure_persons
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))
from src.pure_datasets import find_dataset


@pytest.fixture
def with_id():
    return {
        "uuid": "John Doe",
        "search_string": "doi"
    }

@pytest.fixture
def without_id():
    return {
        "uuid": None,
        "search_string": "doi"
    }

def find_dataset_with_uuid(with_id):
    """Test get_contributors_details with internal contributors."""
    dataset_with_id = [with_id]
    # Mock response from pure_persons.find_person as if it found the person
    result = find_dataset(dataset_with_id)
    assert dataset_with_id["uuid"] in result

    # Additional assertions based on expected result