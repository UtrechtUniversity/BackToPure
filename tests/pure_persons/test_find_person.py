"""
Tests for the find person functionality.

Searches for and retrieves detailed information about a person from an API.

Parameters:
- name (str): The name of the person to be searched. if none => the module will not try to find person on name
- person_ids (dict): A dictionary of identifiers for the person (e.g., UUID, other IDs).
- date (str): A date string used for filtering data. if None => all association ids will be collected
- apikey (str): API key for authentication with the API.
  (Note that the header of the api-call contain the apikey that is loaded in the top of this script)


Returns:
- dict: A dictionary containing detailed information about the person if a unique match is found.
- str: A message indicating no unique person was found if no match or multiple matches are found.

"""

import pytest
import requests_mock

from src.pure_persons import find_person


def test_find_person():
    """
     Test the functionality of test_successful_uuid_search().

     This test ensures that...
    """
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
        assert result['uuid'] == valid_uuidfind_person
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