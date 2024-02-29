import pytest
import requests_mock
from import_RO_pure.pure_utilities import find_person


@pytest.fixture
def mock_api_base_url():
    return 'http://mocked_api_url.com/'  # Replace with the mocked base URL

@pytest.fixture
def mock_headers():
    return {
        "Content-Type": "application/json",
        "accept": "application/json",
        "api-key": "test_api_key"
    }

def test_successful_uuid_search():
    # Replace 'valid_uuid' with a UUID you know should return a valid result
    valid_uuid = 'valid_uuid'

    # Call the find_person function with a known valid UUID
    result = find_person(None, {"uuid": valid_uuid}, None)

    # Assert that the result is as expected
    # This assertion will depend on the actual structure of your returned data
    assert result is not None
    assert result['uuid'] == valid_uuid
    # Add more assertions as needed based on your expected data structure

def test_successful_uuid_search_mock():
    valid_uuid = 'valid_uuid'
    expected_response = {
        "uuid": valid_uuid,
        "name": {"firstName": "John", "lastName": "Doe"}
        # Add more fields to this response based on your actual API response structure
    }
    mock_url = f'https://staging.research-portal.uu.nl/ws/api/persons/{valid_uuid}'

    with requests_mock.Mocker() as m:
        m.get(mock_url, json=expected_response)

        result = find_person(None, {"uuid": valid_uuid}, None)

        # Assert that the result matches the expected mocked response
        assert result['uuid'] == valid_uuid
        assert result['firstName'] == "John"
        assert result['lastName'] == "Doe"
        # Add more assertions as needed


# (Optional) Additional test for unsuccessful UUID search
def test_unsuccessful_uuid_search():
    invalid_uuid = 'invalid_uuid'

    # Call the find_person function with a known invalid UUID
    result = find_person(None, {"uuid": invalid_uuid}, None)

    # Assert that the result is None or as expected in case of invalid UUID
    assert result is None  # or any other expected behavior