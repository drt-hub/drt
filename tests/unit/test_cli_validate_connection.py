from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.models import PostgresDestinationConfig, SlackDestinationConfig

runner = CliRunner()

def test_validate_check_connection_sql_success() -> None:
    """Test validate --check-connection for an SQL destination (success)."""
    # Mocking the registry and destination
    mock_dest = MagicMock()
    mock_dest.test_connection.return_value = None
    
    with patch("drt.connectors.registry.get_destination", return_value=mock_dest), \
         patch("drt.config.parser.load_syncs_safe") as mock_load:
        
        # Setup a mock sync
        mock_sync = MagicMock()
        mock_sync.name = "sql_sync"
        mock_sync.destination = MagicMock(spec=PostgresDestinationConfig)
        
        mock_result = MagicMock()
        mock_result.syncs = [mock_sync]
        mock_result.errors = {}
        mock_result.deprecations = {}
        mock_load.return_value = mock_result
        
        result = runner.invoke(app, ["validate", "--check-connection", "--select", "sql_sync"])
        
        assert result.exit_code == 0
        assert "✓ connection ok" in result.stdout
        mock_dest.test_connection.assert_called_once()

def test_validate_check_connection_sql_failure() -> None:
    """Test validate --check-connection for an SQL destination (failure)."""
    mock_dest = MagicMock()
    mock_dest.test_connection.side_effect = Exception("Connection Refused")
    
    with patch("drt.connectors.registry.get_destination", return_value=mock_dest), \
         patch("drt.config.parser.load_syncs_safe") as mock_load:
        
        mock_sync = MagicMock()
        mock_sync.name = "sql_fail"
        mock_sync.destination = MagicMock(spec=PostgresDestinationConfig)
        
        mock_result = MagicMock()
        mock_result.syncs = [mock_sync]
        mock_result.errors = {}
        mock_result.deprecations = {}
        mock_load.return_value = mock_result
        
        result = runner.invoke(app, ["validate", "--check-connection", "--select", "sql_fail"])
        
        assert result.exit_code == 0 # Validation still passes, only connection failed
        assert "✗ connection failed: Connection Refused" in result.stdout

def test_validate_check_connection_non_sql_skip() -> None:
    """Test validate --check-connection for a non-SQL destination (skip)."""
    with patch("drt.config.parser.load_syncs_safe") as mock_load:
        
        mock_sync = MagicMock()
        mock_sync.name = "slack_sync"
        mock_sync.destination = MagicMock(spec=SlackDestinationConfig)
        
        mock_result = MagicMock()
        mock_result.syncs = [mock_sync]
        mock_result.errors = {}
        mock_result.deprecations = {}
        mock_load.return_value = mock_result
        
        result = runner.invoke(app, ["validate", "--check-connection", "--select", "slack_sync"])
        
        assert result.exit_code == 0
        assert "⏭ connection test skipped (non-SQL)" in result.stdout
