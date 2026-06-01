''' BigQueryDestination unit tests '''

import pytest
from unittest.mock import MagicMock, patch

from drt.destinations.bigquery import (
    BigQueryDestination,
    BigQueryDestinationConfig,
)


# ----------------------------
# Fixtures
# ----------------------------

@pytest.fixture
def config_insert():
    return BigQueryDestinationConfig(
        type="bigquery",
        project="test-project",
        dataset="test_dataset",
        table="test_table",
        method="application_default",
    )


@pytest.fixture
def config_merge():
    return BigQueryDestinationConfig(
        type="bigquery",
        project="test-project",
        dataset="test_dataset",
        table="test_table",
        upsert_key=["id"],
        method="application_default",
    )


@pytest.fixture
def sample_rows():
    return [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]


# ----------------------------
# Client initialization
# ----------------------------

@patch("drt.destinations.bigquery.bigquery.Client")
def test_client_initialization_adc(mock_client, config_insert):
    dest = BigQueryDestination(config_insert)
    mock_client.assert_called_once_with(project="test-project")
    assert dest.client is not None


@patch("drt.destinations.bigquery.service_account.Credentials")
@patch("drt.destinations.bigquery.bigquery.Client")
def test_client_initialization_service_account(
    mock_client, mock_credentials, config_insert
):
    config_insert.method = "service_account"
    config_insert.keyfile = "/fake/key.json"

    mock_credentials.from_service_account_file.return_value = "creds"

    dest = BigQueryDestination(config_insert)

    mock_credentials.from_service_account_file.assert_called_once()
    mock_client.assert_called_once_with(
        project="test-project",
        credentials="creds",
    )


# ----------------------------
# Insert mode
# ----------------------------

@patch("drt.destinations.bigquery.bigquery.Client")
def test_insert_success(mock_client, config_insert, sample_rows):
    mock_instance = MagicMock()
    mock_instance.insert_rows_json.return_value = []
    mock_client.return_value = mock_instance

    dest = BigQueryDestination(config_insert)
    dest.write(sample_rows, mode="insert")

    mock_instance.insert_rows_json.assert_called_once()


@patch("drt.destinations.bigquery.bigquery.Client")
def test_insert_failure(mock_client, config_insert, sample_rows):
    mock_instance = MagicMock()
    mock_instance.insert_rows_json.return_value = [{"error": "fail"}]
    mock_client.return_value = mock_instance

    dest = BigQueryDestination(config_insert)

    with pytest.raises(RuntimeError):
        dest.write(sample_rows, mode="insert")


# ----------------------------
# Merge mode
# ----------------------------

@patch("drt.destinations.bigquery.bigquery.Client")
def test_merge_success(mock_client, config_merge, sample_rows):
    mock_instance = MagicMock()

    # Mock load job
    mock_load_job = MagicMock()
    mock_load_job.result.return_value = None
    mock_instance.load_table_from_json.return_value = mock_load_job

    # Mock query job
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = None
    mock_instance.query.return_value = mock_query_job

    mock_client.return_value = mock_instance

    dest = BigQueryDestination(config_merge)
    dest.write(sample_rows, mode="merge")

    mock_instance.load_table_from_json.assert_called_once()
    mock_instance.query.assert_called_once()
    mock_instance.delete_table.assert_called_once()


@patch("drt.destinations.bigquery.bigquery.Client")
def test_merge_without_key_raises(mock_client, config_insert, sample_rows):
    dest = BigQueryDestination(config_insert)

    with pytest.raises(ValueError):
        dest.write(sample_rows, mode="merge")


# ----------------------------
# General behavior
# ----------------------------

@patch("drt.destinations.bigquery.bigquery.Client")
def test_no_rows_noop(mock_client, config_insert):
    mock_instance = MagicMock()
    mock_client.return_value = mock_instance

    dest = BigQueryDestination(config_insert)
    dest.write([], mode="insert")

    mock_instance.insert_rows_json.assert_not_called()


@patch("drt.destinations.bigquery.bigquery.Client")
def test_invalid_mode_raises(mock_client, config_insert, sample_rows):
    dest = BigQueryDestination(config_insert)

    with pytest.raises(ValueError):
        dest.write(sample_rows, mode="invalid")