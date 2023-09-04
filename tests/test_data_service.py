# third-arty
from unittest.mock import patch, Mock
from fastapi import HTTPException
import pandas as pd
import pytest
from io import BytesIO
from io import StringIO
from fastapi import UploadFile

# project
from services.data_service import fetch_and_aggregate_data_internal, process_upload_internal


def mock_cursor():
    mock = Mock()
    mock.fetchone.return_value = [False]  # version doesn't exist
    return mock


@patch('psycopg2.connect')
def test_version_not_found(mock_connect):
    mock_connect.return_value.cursor.return_value = mock_cursor()

    with pytest.raises(HTTPException) as exc_info:
        fetch_and_aggregate_data_internal(1)

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "Version not found"


def mock_cursor_for_aggregation():
    mock = Mock()
    mock.fetchone.return_value = [True]  # version exists
    mock.fetchall.return_value = [
        (338, "1.2.3", "Data_113", 2022, 0.600997617930686, 1),
        (339, "1.2.3", "Data_113", 2023, 0.906509911207383, 1),
        (340, "1.2.3", "Data_113", 2024, 0.928022536652987, 1),
        (341, "1.2.3", "Data_113", 2025, 0.499511441238251, 1),
    ]
    return mock

@patch('psycopg2.connect')
def test_data_aggregation(mock_connect):
    mock_connect.return_value.cursor.return_value = mock_cursor_for_aggregation()

    result = fetch_and_aggregate_data_internal(1)

    # Convert result to CSV string with correct column names
    df_result = pd.DataFrame(result, columns=["id", "code", "project", "year", "value", "version"])
    csv_data = df_result.to_csv(index=False, sep=";")

    # Encode string to bytes and create a BytesIO object
    buffer = BytesIO(csv_data.encode())
    df = pd.read_csv(buffer, sep=";")

    # Checking aggregated values
    aggregated_value_2022 = df[df['code'] == "1.2.3"]['value'].iloc[0]
    assert aggregated_value_2022 == 0.600997617930686


# mock for DB connection
def mock_db_connection(mocked):
    mock = Mock()
    mock.cursor.return_value = mock
    mock.fetchone.return_value = [1]
    return mock


# mock for file
def mock_upload_file(content: str):
    return UploadFile(file=StringIO(content))


@patch('psycopg2.connect', side_effect=mock_db_connection)
def test_successful_file_upload(mock_connect):
    # mock file preparing
    content = "code;project;2022;2023;2024;2025;2026\n1;Project_1;0.5;0.6;0.7;0.8;0.9"
    file = mock_upload_file(content)

    result = process_upload_internal(file)

    # result check
    assert result == {"status": "success"}


@patch('psycopg2.connect', side_effect=mock_db_connection)
def test_empty_file_upload(mock_connect):
    # empty mock file preparing
    content = "code;project;2022;2023;2024;2025;2026\n"
    file = mock_upload_file(content)

    result = process_upload_internal(file)

    assert result == {"status": "success"}
