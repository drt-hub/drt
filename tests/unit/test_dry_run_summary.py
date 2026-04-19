from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app


def test_run_dry_run_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    # 1. Setup project
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "test_project", "profile": "default"})
    )

    # 2. Setup profiles.yml in mocked config dir
    drt_dir = tmp_path / ".drt"
    drt_dir.mkdir()
    (drt_dir / "profiles.yml").write_text(
        yaml.dump(
            {"default": {"type": "bigquery", "project": "my-project", "dataset": "my_dataset"}}
        )
    )

    # Mock config dir for profile loading
    monkeypatch.setattr("drt.config.credentials._config_dir", lambda override=None: drt_dir)

    # 3. Setup sync
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "my_sync.yml").write_text(
        yaml.dump(
            {
                "name": "my_sync",
                "model": "ref('users')",
                "destination": {
                    "type": "slack",
                    "webhook_url": "https://hooks.slack.com/services/xxx",
                },
                "sync": {"mode": "full"},
            }
        )
    )

    # 4. Mock the engine to return 42 rows
    class FakeResult:
        def __init__(self):
            self.success = 42
            self.failed = 0
            self.skipped = 0
            self.errors = []
            self.row_errors = []
            # Added by PR #345/#347 to SyncResult — keeping the fake in
            # lockstep with the real shape so future refactors that read
            # ``rows_extracted`` unconditionally don't fail this test.
            self.rows_extracted = self.success

    def mock_run_sync(*args, **kwargs):
        return FakeResult()

    # Mock all heavy imports and functions
    monkeypatch.setattr("drt.engine.sync.run_sync", mock_run_sync)
    monkeypatch.setattr("drt.cli.main._get_source", lambda x: None)

    class FakeDest:
        def describe(self):
            return "slack (webhook)"

    monkeypatch.setattr("drt.cli.main._get_destination", lambda x: FakeDest())

    # 5. Run drt run --dry-run
    result = runner.invoke(app, ["run", "--dry-run"])

    # 6. Check output
    assert result.exit_code == 0
    assert "Dry run summary:" in result.output
    assert "Source: bigquery (my-project.my_dataset.users)" in result.output
    assert "Destination: slack (webhook)" in result.output
    assert "Rows to sync: 42" in result.output
    assert "Sync mode: full" in result.output
    # Header should be suppressed
    assert "→ my_sync" not in result.output
