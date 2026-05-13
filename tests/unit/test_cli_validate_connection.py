from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.models import (
    DestinationConfig,
    PostgresDestinationConfig,
    SlackDestinationConfig,
)

runner = CliRunner()


class _ConnectionTestDestination:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    def test_connection(self, config: DestinationConfig) -> None:
        if self.error is not None:
            raise self.error


def test_validate_check_connection_sql_success() -> None:
    """Test validate --check-connection for an SQL destination (success)."""
    mock_dest = _ConnectionTestDestination()
    
    with patch("drt.connectors.registry.get_destination", return_value=mock_dest), \
         patch("drt.config.parser.load_syncs_safe") as mock_load:
        
        mock_sync = MagicMock()
        mock_sync.name = "sql_sync"
        # Use a real class instance to pass isinstance checks in main.py
        mock_sync.destination = PostgresDestinationConfig(
            type="postgres", table="t", upsert_key=["id"],
            host="localhost", dbname="db"
        )
        
        mock_result = MagicMock()
        mock_result.syncs = [mock_sync]
        mock_result.errors = {}
        mock_result.deprecations = {}
        mock_load.return_value = mock_result
        
        result = runner.invoke(app, ["validate", "--check-connection", "--select", "sql_sync"])
        
        assert result.exit_code == 0
        assert "✓ connection ok" in result.stdout

def test_validate_check_connection_sql_failure() -> None:
    """Test validate --check-connection for an SQL destination (failure)."""
    mock_dest = _ConnectionTestDestination(Exception("Conn Error"))
    
    with patch("drt.connectors.registry.get_destination", return_value=mock_dest), \
         patch("drt.config.parser.load_syncs_safe") as mock_load:
        
        mock_sync = MagicMock()
        mock_sync.name = "sql_fail"
        mock_sync.destination = PostgresDestinationConfig(
            type="postgres", table="t", upsert_key=["id"],
            host="localhost", dbname="db"
        )
        
        mock_result = MagicMock()
        mock_result.syncs = [mock_sync]
        mock_result.errors = {}
        mock_result.deprecations = {}
        mock_load.return_value = mock_result
        
        result = runner.invoke(app, ["validate", "--check-connection", "--select", "sql_fail"])
        
        assert result.exit_code == 0
        assert "✗ connection failed: Conn Error" in result.stdout

def test_validate_check_connection_non_sql_skip() -> None:
    """Test validate --check-connection for a non-SQL destination (skip)."""
    with patch("drt.config.parser.load_syncs_safe") as mock_load:
        
        mock_sync = MagicMock()
        mock_sync.name = "slack_sync"
        mock_sync.destination = SlackDestinationConfig(
            type="slack", channel="#c", auth={"type": "token", "token_env": "T"}
        )
        
        mock_result = MagicMock()
        mock_result.syncs = [mock_sync]
        mock_result.errors = {}
        mock_result.deprecations = {}
        mock_load.return_value = mock_result
        
        result = runner.invoke(app, ["validate", "--check-connection", "--select", "slack_sync"])
        
        assert result.exit_code == 0
        assert "⏭ connection test skipped" in result.stdout

def test_validate_check_connection_sql_no_tester_method() -> None:
    """Test case where is_sql is true but test_connection method is missing."""
    mock_dest = object() # No test_connection
    
    with patch("drt.connectors.registry.get_destination", return_value=mock_dest), \
         patch("drt.config.parser.load_syncs_safe") as mock_load:
        
        mock_sync = MagicMock()
        mock_sync.name = "no_method"
        # We must make sure this passes the isinstance check in main.py
        from drt.config.models import PostgresDestinationConfig
        mock_sync.destination = PostgresDestinationConfig(
            type="postgres", table="t", upsert_key=["id"],
            host="localhost", dbname="db"
        )
        
        mock_result = MagicMock()
        mock_result.syncs = [mock_sync]
        mock_result.errors = {}
        mock_result.deprecations = {}
        mock_load.return_value = mock_result
        
        result = runner.invoke(app, ["validate", "--check-connection", "--select", "no_method"])
        
        assert result.exit_code == 0
        assert "✗ connection failed: test_connection method missing" in result.stdout

def test_validate_check_connection_json() -> None:
    """Test validate --check-connection --output json."""
    import json
    mock_dest = _ConnectionTestDestination()
    
    with patch("drt.connectors.registry.get_destination", return_value=mock_dest), \
         patch("drt.config.parser.load_syncs_safe") as mock_load:
        
        mock_sync = MagicMock()
        mock_sync.name = "json_sync"
        mock_sync.destination = PostgresDestinationConfig(
            type="postgres", table="t", upsert_key=["id"],
            host="localhost", dbname="db"
        )
        
        mock_result = MagicMock()
        mock_result.syncs = [mock_sync]
        mock_result.errors = {}
        mock_result.deprecations = {}
        mock_load.return_value = mock_result
        
        result = runner.invoke(app, ["validate", "--check-connection", "--output", "json"])
        
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        
        assert "results" in data
        sync_res = data["results"][0]
        assert sync_res["name"] == "json_sync"
        assert "connection_test" in sync_res
        assert sync_res["connection_test"] == {
            "success": True,
            "error": None,
            "skipped": False
        }

def test_validate_check_connection_skipped_json() -> None:
    """Test validate --check-connection --output json for non-SQL."""
    import json
    with patch("drt.config.parser.load_syncs_safe") as mock_load:
        
        mock_sync = MagicMock()
        mock_sync.name = "skipped_sync"
        mock_sync.destination = SlackDestinationConfig(
            type="slack", channel="#c", auth={"type": "token", "token_env": "T"}
        )
        
        mock_result = MagicMock()
        mock_result.syncs = [mock_sync]
        mock_result.errors = {}
        mock_result.deprecations = {}
        mock_load.return_value = mock_result
        
        result = runner.invoke(app, ["validate", "--check-connection", "--output", "json"])
        
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        
        sync_res = data["results"][0]
        assert sync_res["connection_test"] == {
            "success": None,
            "error": None,
            "skipped": True
        }
