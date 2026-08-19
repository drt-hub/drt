"""Tests for config models, parser, and credentials."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from drt.config.credentials import BigQueryProfile, load_profile, save_profile
from drt.config.models import (
    ApiKeyAuth,
    BasicAuth,
    BearerAuth,
    GoogleSheetsDestinationConfig,
    JiraDestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    ProjectConfig,
    RestApiDestinationConfig,
    SslConfig,
    SyncConfig,
    SyncOptions,
)
from drt.config.parser import expand_env_vars, load_project, load_syncs

# ---------------------------------------------------------------------------
# Auth model discrimination
# ---------------------------------------------------------------------------


def test_bearer_auth_discriminated() -> None:
    config = RestApiDestinationConfig.model_validate(
        {
            "type": "rest_api",
            "url": "https://example.com",
            "auth": {"type": "bearer", "token_env": "MY_TOKEN"},
        }
    )
    assert isinstance(config.auth, BearerAuth)
    assert config.auth.token_env == "MY_TOKEN"


def test_api_key_auth_discriminated() -> None:
    config = RestApiDestinationConfig.model_validate(
        {
            "type": "rest_api",
            "url": "https://example.com",
            "auth": {"type": "api_key", "header": "X-Custom-Key", "value": "secret"},
        }
    )
    assert isinstance(config.auth, ApiKeyAuth)
    assert config.auth.header == "X-Custom-Key"


def test_basic_auth_discriminated() -> None:
    config = RestApiDestinationConfig.model_validate(
        {
            "type": "rest_api",
            "url": "https://example.com",
            "auth": {"type": "basic", "username_env": "USER", "password_env": "PASS"},
        }
    )
    assert isinstance(config.auth, BasicAuth)


def test_no_auth() -> None:
    config = RestApiDestinationConfig.model_validate(
        {
            "type": "rest_api",
            "url": "https://example.com",
        }
    )
    assert config.auth is None


# ---------------------------------------------------------------------------
# ProjectConfig
# ---------------------------------------------------------------------------


def test_project_config_defaults() -> None:
    p = ProjectConfig(name="test")
    assert p.version == "0.1"
    assert p.profile == "default"
    assert p.source is None


def test_project_config_profile_field() -> None:
    p = ProjectConfig(name="test", profile="prod")
    assert p.profile == "prod"


# ---------------------------------------------------------------------------
# Parser — load_project
# ---------------------------------------------------------------------------


def test_load_project(tmp_path: Path) -> None:
    config_file = tmp_path / "drt_project.yml"
    config_file.write_text("name: my-project\nprofile: dev\n")

    project = load_project(tmp_path)
    assert project.name == "my-project"
    assert project.profile == "dev"


def test_load_project_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="drt_project.yml not found"):
        load_project(tmp_path)


def test_load_project_fires_config_changed_audit_event(tmp_path: Path) -> None:
    """#299 / ADR 0008, Codex-review fix: direct load_project() callers
    (e.g. `drt status`, which never calls load_syncs) get the event too,
    not just the load_syncs path."""
    from drt.observability.audit import (
        AuditEvent,
        _reset_audit_logger,
        register_audit_logger,
    )

    captured: list[AuditEvent] = []

    class _Capturing:
        def log_event(self, event: AuditEvent) -> None:
            captured.append(event)

    register_audit_logger(_Capturing())
    try:
        config_file = tmp_path / "drt_project.yml"
        config_file.write_text("name: my-project\nprofile: dev\n")
        load_project(tmp_path)

        assert len(captured) == 1
        assert captured[0].event_type == "config_changed"
        assert captured[0].details["source"] == "load_project"
    finally:
        _reset_audit_logger()


def test_load_project_missing_does_not_fire_audit_event(tmp_path: Path) -> None:
    """A failed load isn't a config read — no event on FileNotFoundError."""
    from drt.observability.audit import (
        AuditEvent,
        _reset_audit_logger,
        register_audit_logger,
    )

    captured: list[AuditEvent] = []

    class _Capturing:
        def log_event(self, event: AuditEvent) -> None:
            captured.append(event)

    register_audit_logger(_Capturing())
    try:
        with pytest.raises(FileNotFoundError):
            load_project(tmp_path)
        assert captured == []
    finally:
        _reset_audit_logger()


def test_config_changed_audit_failure_does_not_break_load_project(tmp_path: Path) -> None:
    """Regression for the Codex-review fire-and-forget fix: a raising
    AuditLogger must not turn a successful config load into a CLI failure."""
    from drt.observability.audit import _reset_audit_logger, register_audit_logger

    class _BrokenSink:
        def log_event(self, event: object) -> None:
            raise RuntimeError("sink unreachable")

    register_audit_logger(_BrokenSink())
    try:
        config_file = tmp_path / "drt_project.yml"
        config_file.write_text("name: my-project\nprofile: dev\n")
        project = load_project(tmp_path)
        assert project.name == "my-project"
    finally:
        _reset_audit_logger()


# ---------------------------------------------------------------------------
# Parser — load_syncs
# ---------------------------------------------------------------------------


def _write_sync(syncs_dir: Path, name: str) -> None:
    syncs_dir.mkdir(exist_ok=True)
    (syncs_dir / f"{name}.yml").write_text(
        f"name: {name}\n"
        "model: ref('table')\n"
        "destination:\n"
        "  type: rest_api\n"
        "  url: https://example.com\n"
    )


def test_load_syncs_empty(tmp_path: Path) -> None:
    assert load_syncs(tmp_path) == []


def test_load_syncs(tmp_path: Path) -> None:
    syncs_dir = tmp_path / "syncs"
    _write_sync(syncs_dir, "alpha")
    _write_sync(syncs_dir, "beta")

    syncs = load_syncs(tmp_path)
    assert len(syncs) == 2
    assert [s.name for s in syncs] == ["alpha", "beta"]


def test_load_syncs_fires_config_changed_audit_event(tmp_path: Path) -> None:
    """#299 / ADR 0008: load_syncs is config_changed's trigger point — a
    successful load emits it exactly once, not per file."""
    from drt.observability.audit import (
        AuditEvent,
        _reset_audit_logger,
        register_audit_logger,
    )

    captured: list[AuditEvent] = []

    class _Capturing:
        def log_event(self, event: AuditEvent) -> None:
            captured.append(event)

    register_audit_logger(_Capturing())
    try:
        syncs_dir = tmp_path / "syncs"
        _write_sync(syncs_dir, "alpha")
        _write_sync(syncs_dir, "beta")

        load_syncs(tmp_path)

        assert len(captured) == 1
        assert captured[0].event_type == "config_changed"
        assert captured[0].details["sync_count"] == 2
    finally:
        _reset_audit_logger()


# ---------------------------------------------------------------------------
# expand_env_vars — generic ${VAR} expansion in YAML data
# ---------------------------------------------------------------------------


def test_expand_env_vars_simple_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_BUCKET", "prod-bucket")
    assert expand_env_vars("${MY_BUCKET}") == "prod-bucket"


def test_expand_env_vars_embedded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROJECT", "analytics")
    assert expand_env_vars("gs://${PROJECT}/data") == "gs://analytics/data"


def test_expand_env_vars_multiple(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOST", "db.example.com")
    monkeypatch.setenv("PORT", "5432")
    assert expand_env_vars("${HOST}:${PORT}") == "db.example.com:5432"


def test_expand_env_vars_nested_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BUCKET", "my-bucket")
    monkeypatch.setenv("API_URL", "https://api.example.com")
    data = {
        "name": "test",
        "sync": {"watermark": {"bucket": "${BUCKET}"}},
        "destination": {"url": "${API_URL}"},
    }
    result = expand_env_vars(data)
    assert result["sync"]["watermark"]["bucket"] == "my-bucket"
    assert result["destination"]["url"] == "https://api.example.com"
    assert result["name"] == "test"  # no substitution needed


def test_expand_env_vars_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TAG", "production")
    data = {"tags": ["static", "${TAG}"]}
    result = expand_env_vars(data)
    assert result["tags"] == ["static", "production"]


def test_expand_env_vars_non_string_unchanged() -> None:
    data = {"batch_size": 100, "enabled": True, "ratio": 0.5, "empty": None}
    assert expand_env_vars(data) == data


def test_expand_env_vars_missing_raises() -> None:
    with pytest.raises(ValueError, match="NONEXISTENT_VAR"):
        expand_env_vars("${NONEXISTENT_VAR}")


def test_expand_env_vars_no_placeholders() -> None:
    assert expand_env_vars("plain string") == "plain string"


def test_load_syncs_expands_env_vars(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Environment variables in sync YAML are expanded before validation."""
    monkeypatch.setenv("TEST_API_URL", "https://expanded.example.com")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "env_sync.yml").write_text(
        "name: env-sync\nmodel: SELECT 1\ndestination:\n  type: rest_api\n  url: ${TEST_API_URL}\n"
    )
    syncs = load_syncs(tmp_path)
    assert len(syncs) == 1
    assert syncs[0].destination.url == "https://expanded.example.com"  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Credentials — load_profile / save_profile
# ---------------------------------------------------------------------------


def test_save_and_load_profile(tmp_path: Path) -> None:
    profile = BigQueryProfile(
        type="bigquery",
        project="my-project",
        dataset="my_dataset",
        method="application_default",
    )
    save_profile("dev", profile, config_dir=tmp_path)
    loaded = load_profile("dev", config_dir=tmp_path)

    assert loaded.project == "my-project"
    assert loaded.dataset == "my_dataset"
    assert loaded.method == "application_default"


def test_load_profile_bigquery_location(tmp_path: Path) -> None:
    (tmp_path / "profiles.yml").write_text(
        "dev:\n  type: bigquery\n  project: p\n  dataset: d\n  location: asia-northeast1\n"
    )
    loaded = load_profile("dev", config_dir=tmp_path)
    assert loaded.location == "asia-northeast1"


def test_load_profile_bigquery_location_default(tmp_path: Path) -> None:
    (tmp_path / "profiles.yml").write_text("dev:\n  type: bigquery\n  project: p\n  dataset: d\n")
    loaded = load_profile("dev", config_dir=tmp_path)
    assert loaded.location == "US"


def test_load_profile_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="profiles.yml not found"):
        load_profile("dev", config_dir=tmp_path)


def test_load_profile_missing_key(tmp_path: Path) -> None:
    (tmp_path / "profiles.yml").write_text("prod:\n  type: bigquery\n  project: x\n  dataset: y\n")
    with pytest.raises(KeyError, match="Profile 'dev' not found"):
        load_profile("dev", config_dir=tmp_path)


def test_save_profile_appends(tmp_path: Path) -> None:
    existing = BigQueryProfile(type="bigquery", project="p1", dataset="d1")
    save_profile("dev", existing, config_dir=tmp_path)

    new_profile = BigQueryProfile(type="bigquery", project="p2", dataset="d2")
    save_profile("prod", new_profile, config_dir=tmp_path)

    profiles_path = tmp_path / "profiles.yml"
    data = yaml.safe_load(profiles_path.read_text())
    assert "profiles" in data
    assert "dev" in data["profiles"]
    assert "prod" in data["profiles"]


# ---------------------------------------------------------------------------
# Google Sheets destination config
# ---------------------------------------------------------------------------


def test_google_sheets_destination_config_parses() -> None:
    raw = {
        "name": "export_to_sheets",
        "model": "ref('users')",
        "destination": {
            "type": "google_sheets",
            "spreadsheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms",
            "sheet": "Sheet1",
            "mode": "overwrite",
        },
    }
    cfg = SyncConfig(**raw)
    assert cfg.destination.type == "google_sheets"
    assert cfg.destination.spreadsheet_id == "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"
    assert cfg.destination.sheet == "Sheet1"
    assert cfg.destination.mode == "overwrite"


# ---------------------------------------------------------------------------
# SyncOptions — upsert mode
# ---------------------------------------------------------------------------


def test_sync_options_upsert_mode_accepted() -> None:
    """mode='upsert' is valid and behaves like 'full' (no cursor_field required)."""
    opts = SyncOptions(mode="upsert")
    assert opts.mode == "upsert"
    assert opts.cursor_field is None


def test_sync_options_full_mode_still_works() -> None:
    """Backward compat: mode='full' remains the default."""
    opts = SyncOptions()
    assert opts.mode == "full"


# ---------------------------------------------------------------------------
# SyncOptions — Dead Letter Queue (#278)
# ---------------------------------------------------------------------------


def test_dlq_defaults_to_disabled() -> None:
    """DLQ is opt-in: absent config means no queue (no surprise disk writes)."""
    assert SyncOptions().dlq is None


def test_dlq_config_parses_from_sync() -> None:
    from drt.config.models import DLQConfig

    cfg = SyncConfig(
        **{
            "name": "s",
            "model": "ref('t')",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {"dlq": {"enabled": True, "max_records": 500}},
        }
    )
    assert isinstance(cfg.sync.dlq, DLQConfig)
    assert cfg.sync.dlq.enabled is True
    assert cfg.sync.dlq.max_records == 500


def test_dlq_max_records_defaults() -> None:
    from drt.config.models import DLQConfig

    dlq = DLQConfig(enabled=True)
    assert dlq.max_records == 10_000


def test_dlq_negative_max_records_rejected() -> None:
    import pytest

    from drt.config.models import DLQConfig

    with pytest.raises(ValueError, match="dlq.max_records must be >= 0"):
        DLQConfig(max_records=-1)


def test_sync_options_upsert_in_sync_config() -> None:
    """mode='upsert' works end-to-end inside a SyncConfig."""
    raw = {
        "name": "upsert_sync",
        "model": "ref('scores')",
        "destination": {
            "type": "rest_api",
            "url": "https://example.com/api",
        },
        "sync": {"mode": "upsert"},
    }
    cfg = SyncConfig(**raw)
    assert cfg.sync.mode == "upsert"


def test_sync_options_replace_mode_accepted() -> None:
    """mode='replace' is valid and does not require cursor_field or upsert_key."""
    opts = SyncOptions(mode="replace")
    assert opts.mode == "replace"
    assert opts.cursor_field is None


def test_sync_options_replace_in_sync_config() -> None:
    """mode='replace' works end-to-end inside a SyncConfig."""
    raw = {
        "name": "replace_sync",
        "model": "ref('sessions')",
        "destination": {
            "type": "rest_api",
            "url": "https://example.com/api",
        },
        "sync": {"mode": "replace"},
    }
    cfg = SyncConfig(**raw)
    assert cfg.sync.mode == "replace"


def test_watermark_config_gcs() -> None:
    opts = SyncOptions(
        mode="incremental",
        cursor_field="updated_at",
        watermark={
            "storage": "gcs",
            "bucket": "my-bucket",
            "key": "wm/sync.json",
        },
    )
    assert opts.watermark is not None
    assert opts.watermark.storage == "gcs"
    assert opts.watermark.bucket == "my-bucket"


def test_watermark_config_bigquery() -> None:
    opts = SyncOptions(
        mode="incremental",
        cursor_field="updated_at",
        watermark={
            "storage": "bigquery",
            "project": "my-proj",
            "dataset": "my_ds",
        },
    )
    assert opts.watermark is not None
    assert opts.watermark.storage == "bigquery"


def test_watermark_config_local_default() -> None:
    opts = SyncOptions(
        mode="incremental",
        cursor_field="updated_at",
        watermark={"storage": "local"},
    )
    assert opts.watermark is not None
    assert opts.watermark.storage == "local"


def test_watermark_config_none_by_default() -> None:
    opts = SyncOptions(mode="full")
    assert opts.watermark is None


def test_ssl_config_defaults() -> None:
    ssl = SslConfig()
    assert ssl.enabled is False
    assert ssl.ca_env is None
    assert ssl.cert_env is None
    assert ssl.key_env is None


def test_ssl_config_full() -> None:
    ssl = SslConfig(enabled=True, ca_env="SSL_CA", cert_env="SSL_CERT", key_env="SSL_KEY")
    assert ssl.enabled is True
    assert ssl.ca_env == "SSL_CA"


def test_postgres_destination_with_ssl() -> None:
    cfg = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="testdb",
        table="t",
        upsert_key=["id"],
        ssl=SslConfig(enabled=True, ca_env="PG_SSL_CA"),
    )
    assert cfg.ssl is not None
    assert cfg.ssl.enabled is True
    assert cfg.ssl.ca_env == "PG_SSL_CA"


def test_postgres_destination_without_ssl() -> None:
    cfg = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="testdb",
        table="t",
        upsert_key=["id"],
    )
    assert cfg.ssl is None


def test_mysql_destination_with_ssl() -> None:
    cfg = MySQLDestinationConfig(
        type="mysql",
        host="localhost",
        dbname="testdb",
        table="t",
        upsert_key=["id"],
        ssl=SslConfig(enabled=True, ca_env="MYSQL_SSL_CA", cert_env="MYSQL_SSL_CERT"),
    )
    assert cfg.ssl is not None
    assert cfg.ssl.enabled is True
    assert cfg.ssl.ca_env == "MYSQL_SSL_CA"


def test_google_sheets_destination_defaults() -> None:
    cfg = GoogleSheetsDestinationConfig(
        type="google_sheets",
        spreadsheet_id="abc123",
    )
    assert cfg.sheet == "Sheet1"
    assert cfg.mode == "overwrite"
    assert cfg.credentials_path is None
    assert cfg.credentials_env is None


def test_jira_destination_defaults() -> None:
    cfg = JiraDestinationConfig(
        type="jira",
        base_url_env="JIRA_BASE_URL",
        email_env="JIRA_EMAIL",
        token_env="JIRA_API_TOKEN",
        project_key="ENG",
        summary_template="Alert: {{ row.metric }}",
        description_template="Value: {{ row.value }}",
    )
    assert cfg.type == "jira"
    assert cfg.issue_type == "Task"
    assert cfg.issue_id_field == "issue_id"


# ---------------------------------------------------------------------------
# PostgresDestinationConfig — connection_string_env
# ---------------------------------------------------------------------------


def test_postgres_config_connection_string_env() -> None:
    """connection_string_env should be accepted without host/dbname."""
    cfg = PostgresDestinationConfig(
        type="postgres",
        connection_string_env="DATABASE_URL",
        table="public.scores",
        upsert_key=["id"],
    )
    assert cfg.connection_string_env == "DATABASE_URL"
    assert cfg.host is None
    assert cfg.dbname is None


def test_postgres_config_individual_params() -> None:
    """Individual host/dbname params should still work (backward compat)."""
    cfg = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="analytics",
        table="public.scores",
        upsert_key=["id"],
    )
    assert cfg.host == "localhost"
    assert cfg.dbname == "analytics"
    assert cfg.connection_string_env is None


def test_postgres_config_no_connection_method_raises() -> None:
    """Validation should fail when no connection method is provided."""
    with pytest.raises(ValueError, match="connection_string_env"):
        PostgresDestinationConfig(
            type="postgres",
            table="public.scores",
            upsert_key=["id"],
        )


# ---------------------------------------------------------------------------
# MySQLDestinationConfig — connection_string_env
# ---------------------------------------------------------------------------


def test_mysql_config_connection_string_env() -> None:
    """connection_string_env should be accepted without host/dbname."""
    cfg = MySQLDestinationConfig(
        type="mysql",
        connection_string_env="MYSQL_URL",
        table="scores",
        upsert_key=["id"],
    )
    assert cfg.connection_string_env == "MYSQL_URL"
    assert cfg.host is None
    assert cfg.dbname is None


def test_mysql_config_individual_params() -> None:
    """Individual host/dbname params should still work (backward compat)."""
    cfg = MySQLDestinationConfig(
        type="mysql",
        host="localhost",
        dbname="analytics",
        table="scores",
        upsert_key=["id"],
    )
    assert cfg.host == "localhost"
    assert cfg.dbname == "analytics"
    assert cfg.connection_string_env is None


def test_mysql_config_no_connection_method_raises() -> None:
    """Validation should fail when no connection method is provided."""
    with pytest.raises(ValueError, match="connection_string_env"):
        MySQLDestinationConfig(
            type="mysql",
            table="scores",
            upsert_key=["id"],
        )


# ---------------------------------------------------------------------------
# SyncConfig tests
# ---------------------------------------------------------------------------


def test_sync_config_with_tests() -> None:
    data = {
        "name": "s",
        "model": "SELECT 1",
        "destination": {
            "type": "rest_api",
            "url": "http://x",
            "method": "POST",
        },
        "tests": [
            {"row_count": {"min": 1}},
            {"not_null": {"columns": ["id", "name"]}},
        ],
    }
    sync = SyncConfig.model_validate(data)
    assert len(sync.tests) == 2
    assert sync.tests[0].row_count is not None
    assert sync.tests[1].not_null is not None


def test_sync_config_without_tests() -> None:
    data = {
        "name": "s",
        "model": "SELECT 1",
        "destination": {
            "type": "rest_api",
            "url": "http://x",
            "method": "POST",
        },
    }
    sync = SyncConfig.model_validate(data)
    assert sync.tests == []


# ---------------------------------------------------------------------------
# Alerts config (sync failure alerts — #414)
# ---------------------------------------------------------------------------


class TestAlertsConfig:
    def test_default_alerts_is_none(self) -> None:
        sync = SyncConfig(
            name="t",
            model="select 1",
            destination=RestApiDestinationConfig(type="rest_api", url="https://x"),
        )
        assert sync.alerts is None

    def test_slack_alert_parsed_via_discriminator(self) -> None:
        from drt.config.models import AlertsConfig, SlackAlertConfig

        cfg = AlertsConfig(
            on_failure=[{"type": "slack", "webhook_url": "https://hooks.slack.com/x"}]
        )
        assert isinstance(cfg.on_failure[0], SlackAlertConfig)

    def test_webhook_alert_parsed_via_discriminator(self) -> None:
        from drt.config.models import AlertsConfig, WebhookAlertConfig

        cfg = AlertsConfig(on_failure=[{"type": "webhook", "url": "https://example.com/hook"}])
        assert isinstance(cfg.on_failure[0], WebhookAlertConfig)

    def test_unknown_alert_type_rejected(self) -> None:
        from drt.config.models import AlertsConfig

        with pytest.raises(ValidationError):
            AlertsConfig(on_failure=[{"type": "pagerduty", "key": "x"}])

    def test_slack_requires_webhook_url_or_env(self) -> None:
        from drt.config.models import SlackAlertConfig

        with pytest.raises(ValueError, match="webhook_url"):
            SlackAlertConfig(type="slack")

    def test_webhook_requires_url_or_env(self) -> None:
        from drt.config.models import WebhookAlertConfig

        with pytest.raises(ValueError, match="url"):
            WebhookAlertConfig(type="webhook")


# ---------------------------------------------------------------------------
# Replace strategy (zero-downtime swap — #338)
# ---------------------------------------------------------------------------


class TestReplaceStrategy:
    def test_default_replace_strategy_is_truncate(self) -> None:
        opts = SyncOptions(mode="replace")
        assert opts.replace_strategy == "truncate"

    def test_replace_strategy_swap_accepted(self) -> None:
        opts = SyncOptions(mode="replace", replace_strategy="swap")
        assert opts.replace_strategy == "swap"

    def test_replace_strategy_invalid_value_rejected(self) -> None:
        with pytest.raises(ValidationError):
            SyncOptions(mode="replace", replace_strategy="hotswap")  # type: ignore[arg-type]

    def test_replace_strategy_swap_requires_replace_mode(self) -> None:
        with pytest.raises(ValueError, match="replace_strategy"):
            SyncOptions(mode="full", replace_strategy="swap")


class TestMirrorConfig:
    """``sync.mirror`` — tracked mirror config surface (#686)."""

    def test_defaults_to_destination_strategy(self) -> None:
        from drt.config.models import MirrorConfig

        opts = SyncOptions(mode="mirror", mirror=MirrorConfig())
        assert opts.mirror is not None
        assert opts.mirror.strategy == "destination"

    def test_tracked_strategy_accepted(self) -> None:
        opts = SyncOptions(mode="mirror", mirror={"strategy": "tracked"})
        assert opts.mirror is not None
        assert opts.mirror.strategy == "tracked"

    def test_unknown_strategy_rejected(self) -> None:
        with pytest.raises(ValidationError):
            SyncOptions(mode="mirror", mirror={"strategy": "census"})

    def test_mirror_block_requires_mirror_mode(self) -> None:
        with pytest.raises(ValueError, match="sync.mirror"):
            SyncOptions(mode="full", mirror={"strategy": "tracked"})

    def test_sync_config_injects_sync_name_into_options(self) -> None:
        cfg = SyncConfig(
            name="scores_sync",
            model="SELECT 1",
            destination={
                "type": "postgres",
                "host": "localhost",
                "dbname": "db",
                "user": "u",
                "password": "p",
                "table": "public.scores",
                "upsert_key": ["id"],
            },
            sync={"mode": "mirror", "mirror": {"strategy": "tracked"}},
        )
        assert cfg.sync._sync_name == "scores_sync"

    def test_sync_name_defaults_to_none_without_sync_config(self) -> None:
        assert SyncOptions(mode="mirror")._sync_name is None


class TestMirrorScope:
    """``sync.mirror.scope`` — scoped mirror deletes (#687)."""

    def test_scope_accepted_with_destination_strategy(self) -> None:
        opts = SyncOptions(mode="mirror", mirror={"scope": ["parent_id"]})
        assert opts.mirror is not None
        assert opts.mirror.scope == ["parent_id"]
        assert opts.mirror.strategy == "destination"

    def test_scope_with_tracked_strategy_accepted_at_config_level(self) -> None:
        """#694 — tracked+scope composition is now valid config; the
        scope-subset-of-upsert_key constraint is checked destination-side
        (BaseSqlDestination._validate_mirror_scope, needs `upsert_key`,
        which isn't visible from MirrorConfig alone)."""
        opts = SyncOptions(
            mode="mirror",
            mirror={"strategy": "tracked", "scope": ["parent_id"]},
        )
        assert opts.mirror is not None
        assert opts.mirror.strategy == "tracked"
        assert opts.mirror.scope == ["parent_id"]

    def test_empty_scope_list_rejected(self) -> None:
        with pytest.raises(ValidationError):
            SyncOptions(mode="mirror", mirror={"scope": []})


class TestSnowflakeKeyPairAuth:
    """Snowflake key-pair auth config surface (#737)."""

    def _base(self, **auth: str) -> dict:
        return {
            "type": "snowflake",
            "account_env": "SF_ACCOUNT",
            "user_env": "SF_USER",
            "database": "DB",
            "schema": "PUBLIC",
            "table": "T",
            "warehouse": "WH",
            **auth,
        }

    def test_password_only_still_valid(self) -> None:
        from drt.config.models import SnowflakeDestinationConfig

        cfg = SnowflakeDestinationConfig(**self._base(password_env="SF_PASS"))
        assert cfg.password_env == "SF_PASS"
        assert cfg.private_key_env is None

    def test_private_key_only_valid(self) -> None:
        from drt.config.models import SnowflakeDestinationConfig

        cfg = SnowflakeDestinationConfig(**self._base(private_key_env="SF_PK"))
        assert cfg.private_key_env == "SF_PK"
        assert cfg.password_env is None

    def test_neither_auth_rejected(self) -> None:
        from drt.config.models import SnowflakeDestinationConfig

        with pytest.raises(ValueError, match="private_key_env.*or password_env"):
            SnowflakeDestinationConfig(**self._base())


# ---------------------------------------------------------------------------
# RateLimitConfig — burst + fractional rps (#769)
# ---------------------------------------------------------------------------


class TestRateLimitConfig:
    """``burst`` is opt-in and ``requests_per_second`` accepts floats (#769)."""

    def test_rate_limit_config_accepts_burst_and_fractional_rps(self) -> None:
        from drt.config.sync_options import RateLimitConfig

        rl = RateLimitConfig(requests_per_second=2.5, burst=5)
        assert rl.requests_per_second == 2.5
        assert rl.burst == 5

    def test_rate_limit_config_burst_defaults_to_none(self) -> None:
        """None = interval-only behaviour, i.e. exactly what shipped before."""
        from drt.config.sync_options import RateLimitConfig

        assert RateLimitConfig().burst is None

    def test_rate_limit_config_default_rps_unchanged(self) -> None:
        from drt.config.sync_options import RateLimitConfig

        assert RateLimitConfig().requests_per_second == 10

    def test_burst_must_be_at_least_one(self) -> None:
        from drt.config.sync_options import RateLimitConfig

        with pytest.raises(ValidationError):
            RateLimitConfig(burst=0)

    def test_burst_rejects_negative(self) -> None:
        from drt.config.sync_options import RateLimitConfig

        with pytest.raises(ValidationError):
            RateLimitConfig(burst=-3)


# ---------------------------------------------------------------------------
# rate_limit_key() — endpoint-scoped limiter identity (#769)
# ---------------------------------------------------------------------------


class TestRateLimitKey:
    """``rate_limit_key()`` names the real endpoint sharing a vendor quota.

    The key exists precisely because ``describe()`` / ``describe_safe()``
    cannot serve: the former collides for Slack (every config describes as the
    literal ``"webhook"``) and splits for HubSpot (``object_type`` is in the
    detail though one portal shares one quota); the latter is deliberately
    lossy for docs safety (#696). Each test below pins one of those traps so a
    future change that reintroduces it fails here rather than in production.
    """

    def test_hubspot_rate_limit_key_ignores_object_type(self) -> None:
        """Same portal, different object types share one quota — keys must match."""
        from drt.config.base import BearerAuth
        from drt.config.destinations_saas import HubSpotDestinationConfig

        auth = BearerAuth(type="bearer", token_env="HUBSPOT_TOKEN")
        contacts = HubSpotDestinationConfig(type="hubspot", object_type="contacts", auth=auth)
        deals = HubSpotDestinationConfig(type="hubspot", object_type="deals", auth=auth)

        assert contacts.rate_limit_key() == deals.rate_limit_key()
        # And describe() — the tempting shortcut — genuinely differs, so this
        # test is guarding a real divergence rather than a tautology.
        assert contacts.describe() != deals.describe()

    def test_hubspot_rate_limit_key_distinguishes_portals(self) -> None:
        """Different tokens are different portals with independent quotas."""
        from drt.config.base import BearerAuth
        from drt.config.destinations_saas import HubSpotDestinationConfig

        a = HubSpotDestinationConfig(
            type="hubspot", auth=BearerAuth(type="bearer", token_env="PORTAL_A")
        )
        b = HubSpotDestinationConfig(
            type="hubspot", auth=BearerAuth(type="bearer", token_env="PORTAL_B")
        )

        assert a.rate_limit_key() != b.rate_limit_key()

    def test_slack_rate_limit_key_distinguishes_webhooks(self) -> None:
        """describe() returns the literal "webhook" for every Slack config — the key must not."""
        from drt.config.destinations_saas import SlackDestinationConfig

        a = SlackDestinationConfig(type="slack", webhook_url_env="HOOK_A")
        b = SlackDestinationConfig(type="slack", webhook_url_env="HOOK_B")

        assert a.rate_limit_key() != b.rate_limit_key()
        # The trap, stated: describe() would have collided these into one bucket.
        assert a.describe() == b.describe()

    def test_slack_literal_webhook_url_is_not_exposed_in_the_key(self) -> None:
        """A literal webhook URL is a credential — nothing derived from it,
        not even a digest, may reach the key.

        Inline literals collapse onto ``LITERAL_CREDENTIAL_KEY``, so two
        different inline webhooks share one bucket. That is deliberate: a
        shared bucket is *stricter* than two, so the failure mode is a slower
        sync rather than a 429. Naming the env var is how you get per-account
        buckets — pinned by the sibling test below.
        """
        from drt.config.destinations_saas import SlackDestinationConfig

        url = "https://hooks.slack.com/services/T000/B000/XXXXSECRETXXXX"
        cfg = SlackDestinationConfig(type="slack", webhook_url=url)

        key = cfg.rate_limit_key()
        assert url not in key
        assert "XXXXSECRETXXXX" not in key
        # Stable for the same config...
        assert key == SlackDestinationConfig(type="slack", webhook_url=url).rate_limit_key()
        # ...and errs toward one shared (stricter) bucket for inline literals.
        assert (
            key
            == SlackDestinationConfig(
                type="slack", webhook_url="https://hooks.slack.com/services/T000/B000/OTHER"
            ).rate_limit_key()
        )

    def test_slack_env_named_webhooks_get_separate_buckets(self) -> None:
        """The documented way to get per-account buckets: name the env var."""
        from drt.config.destinations_saas import SlackDestinationConfig

        a = SlackDestinationConfig(type="slack", webhook_url_env="HOOK_A")
        b = SlackDestinationConfig(type="slack", webhook_url_env="HOOK_B")
        assert a.rate_limit_key() != b.rate_limit_key()

    def test_airtable_rate_limit_key_ignores_table_name(self) -> None:
        """Same base, different tables share a quota."""
        from drt.config.destinations_saas import AirtableDestinationConfig

        users = AirtableDestinationConfig(
            type="airtable", base_id="appABC", table_name="Users", access_token_env="AT"
        )
        orders = AirtableDestinationConfig(
            type="airtable", base_id="appABC", table_name="Orders", access_token_env="AT"
        )

        assert users.rate_limit_key() == orders.rate_limit_key()
        assert users.describe() != orders.describe()

    def test_airtable_rate_limit_key_distinguishes_bases(self) -> None:
        from drt.config.destinations_saas import AirtableDestinationConfig

        a = AirtableDestinationConfig(
            type="airtable", base_id="appABC", table_name="Users", access_token_env="AT"
        )
        b = AirtableDestinationConfig(
            type="airtable", base_id="appXYZ", table_name="Users", access_token_env="AT"
        )

        assert a.rate_limit_key() != b.rate_limit_key()

    def test_zendesk_rate_limit_key_is_the_subdomain(self) -> None:
        """One Zendesk subdomain is one account with one quota, whatever the object."""
        from drt.config.destinations_saas import ZendeskDestinationConfig

        users = ZendeskDestinationConfig(type="zendesk", subdomain="acme", object="user")
        orgs = ZendeskDestinationConfig(type="zendesk", subdomain="acme", object="organization")
        other = ZendeskDestinationConfig(type="zendesk", subdomain="globex", object="user")

        assert users.rate_limit_key() == orgs.rate_limit_key()
        assert users.rate_limit_key() != other.rate_limit_key()

    def test_rest_api_rate_limit_key_is_the_host_not_the_path(self) -> None:
        """Paths vary per sync; the published quota is per host."""
        from drt.config.destinations_saas import RestApiDestinationConfig

        users = RestApiDestinationConfig(type="rest_api", url="https://api.example.com/v1/users")
        orders = RestApiDestinationConfig(type="rest_api", url="https://api.example.com/v2/orders")
        elsewhere = RestApiDestinationConfig(type="rest_api", url="https://api.other.com/v1/users")

        assert users.rate_limit_key() == orders.rate_limit_key()
        assert users.rate_limit_key() != elsewhere.rate_limit_key()

    def test_rest_api_rate_limit_key_separates_ports(self) -> None:
        """netloc includes the port — two services on one host are two endpoints."""
        from drt.config.destinations_saas import RestApiDestinationConfig

        a = RestApiDestinationConfig(type="rest_api", url="http://localhost:8080/hook")
        b = RestApiDestinationConfig(type="rest_api", url="http://localhost:9090/hook")

        assert a.rate_limit_key() != b.rate_limit_key()

    def test_notion_rate_limit_key_ignores_database_id(self) -> None:
        """Notion's 3 req/s cap is per integration token, not per database."""
        from drt.config.base import BearerAuth
        from drt.config.destinations_saas import NotionDestinationConfig

        auth = BearerAuth(type="bearer", token_env="NOTION_TOKEN")
        a = NotionDestinationConfig(type="notion", database_id="db-1", auth=auth)
        b = NotionDestinationConfig(type="notion", database_id="db-2", auth=auth)

        assert a.rate_limit_key() == b.rate_limit_key()

    def test_klaviyo_rate_limit_key_is_the_account(self) -> None:
        from drt.config.destinations_saas import KlaviyoDestinationConfig

        a = KlaviyoDestinationConfig(type="klaviyo", api_key_env="KLAVIYO_A", list_id="L1")
        b = KlaviyoDestinationConfig(type="klaviyo", api_key_env="KLAVIYO_A", list_id="L2")
        c = KlaviyoDestinationConfig(type="klaviyo", api_key_env="KLAVIYO_B", list_id="L1")

        assert a.rate_limit_key() == b.rate_limit_key()  # same account, different lists
        assert a.rate_limit_key() != c.rate_limit_key()

    def test_jira_rate_limit_key_is_the_site_not_the_project(self) -> None:
        """One Jira site shares a quota across projects."""
        from drt.config.destinations_saas import JiraDestinationConfig

        def _cfg(project_key: str, base_url_env: str = "JIRA_URL") -> JiraDestinationConfig:
            return JiraDestinationConfig(
                type="jira",
                base_url_env=base_url_env,
                email_env="JIRA_EMAIL",
                token_env="JIRA_TOKEN",
                project_key=project_key,
                summary_template="s",
                description_template="d",
            )

        assert _cfg("ENG").rate_limit_key() == _cfg("OPS").rate_limit_key()
        assert _cfg("ENG").rate_limit_key() != _cfg("ENG", "OTHER_JIRA_URL").rate_limit_key()

    def test_sendgrid_rate_limit_key_ignores_the_sender(self) -> None:
        """The quota belongs to the API key, not the From address."""
        from drt.config.base import BearerAuth
        from drt.config.destinations_saas import SendGridDestinationConfig

        def _cfg(from_email: str, token_env: str = "SG_KEY") -> SendGridDestinationConfig:
            return SendGridDestinationConfig(
                type="sendgrid",
                from_email=from_email,
                subject_template="s",
                body_template="b",
                auth=BearerAuth(type="bearer", token_env=token_env),
            )

        assert _cfg("a@example.com").rate_limit_key() == _cfg("b@example.com").rate_limit_key()
        assert _cfg("a@x.com").rate_limit_key() != _cfg("a@x.com", "SG2").rate_limit_key()

    def test_linear_rate_limit_key_is_the_api_key(self) -> None:
        from drt.config.base import BearerAuth
        from drt.config.destinations_saas import LinearDestinationConfig

        def _cfg(token_env: str, team_id: str) -> LinearDestinationConfig:
            return LinearDestinationConfig(
                type="linear",
                team_id=team_id,
                title_template="t",
                description_template="d",
                auth=BearerAuth(type="bearer", token_env=token_env),
            )

        same_key_other_team = _cfg("LINEAR_A", "team-2").rate_limit_key()
        other_key = _cfg("LINEAR_B", "team-1").rate_limit_key()
        baseline = _cfg("LINEAR_A", "team-1").rate_limit_key()

        assert baseline == same_key_other_team
        assert baseline != other_key

    def test_intercom_rate_limit_key_follows_the_auth(self) -> None:
        from drt.config.base import BearerAuth
        from drt.config.destinations_saas import IntercomDestinationConfig

        def _cfg(token_env: str) -> IntercomDestinationConfig:
            return IntercomDestinationConfig(
                type="intercom",
                auth=BearerAuth(type="bearer", token_env=token_env),
                properties_template="{}",
            )

        assert _cfg("IC_A").rate_limit_key() == _cfg("IC_A").rate_limit_key()
        assert _cfg("IC_A").rate_limit_key() != _cfg("IC_B").rate_limit_key()

    def test_rate_limit_key_defaults_to_type(self) -> None:
        """A connector with no override still gets a stable key."""
        from drt.config.destinations_saas import GoogleSheetsDestinationConfig

        cfg = GoogleSheetsDestinationConfig(type="google_sheets", spreadsheet_id="sheet-1")
        assert cfg.rate_limit_key() == "google_sheets"

    def test_default_key_is_stable_across_instances(self) -> None:
        """Two equivalent configs must land in the same bucket, run after run."""
        from drt.config.destinations_saas import GoogleSheetsDestinationConfig

        a = GoogleSheetsDestinationConfig(type="google_sheets", spreadsheet_id="s1")
        b = GoogleSheetsDestinationConfig(type="google_sheets", spreadsheet_id="s2")
        assert a.rate_limit_key() == b.rate_limit_key()

    def test_every_destination_config_exposes_rate_limit_key(self) -> None:
        """The registry calls this on any config in the union — including the 10
        SaaS classes that inherit BaseModel directly rather than
        DescribableConfig. A connector added later without the method would
        break the registry, so sweep the whole union here."""
        import typing

        from drt.config.sync_options import DestinationConfig

        members = typing.get_args(typing.get_args(DestinationConfig)[0])
        assert len(members) > 30, "union unexpectedly small — did the import shape change?"
        missing = [m.__name__ for m in members if not hasattr(m, "rate_limit_key")]
        assert missing == []

    def test_keys_are_prefixed_by_type_so_connectors_cannot_collide(self) -> None:
        """Two different connectors must never share a bucket by accident."""
        from drt.config.destinations_saas import (
            AirtableDestinationConfig,
            SlackDestinationConfig,
        )

        slack = SlackDestinationConfig(type="slack", webhook_url_env="SHARED")
        airtable = AirtableDestinationConfig(
            type="airtable", base_id="SHARED", table_name="t", access_token_env="AT"
        )

        assert slack.rate_limit_key() != airtable.rate_limit_key()
        assert slack.rate_limit_key().startswith("slack:")
        assert airtable.rate_limit_key().startswith("airtable:")


# ---------------------------------------------------------------------------
# destination-level rate_limit override (#769)
# ---------------------------------------------------------------------------


def _configs_with_retry() -> list[type]:
    """Every destination config class that carries a ``retry`` override.

    ``retry`` marks the connectors that actually issue paced HTTP calls, which
    is exactly the set that needs a ``rate_limit`` sibling. Deriving the list
    from the union rather than hard-coding it means a connector added later is
    swept automatically.
    """
    import typing

    from drt.config.sync_options import DestinationConfig

    members = typing.get_args(typing.get_args(DestinationConfig)[0])
    return [m for m in members if "retry" in m.model_fields]


class TestDestinationRateLimitOverride:
    """``destination.rate_limit`` mirrors ``destination.retry`` (#769).

    The field is repeated per class rather than hoisted to a shared base
    because only some of these inherit ``DescribableConfig`` — normalizing the
    ``BaseModel``-direct ones is a separate refactor. The sweep below is what
    keeps that duplication honest.
    """

    def test_retry_capable_configs_all_expose_rate_limit(self) -> None:
        """Any config with ``retry`` must also have ``rate_limit``.

        A connector added later that copies the ``retry`` line but forgets
        ``rate_limit`` would silently ignore destination-level overrides —
        this fails loudly instead.
        """
        with_retry = _configs_with_retry()
        assert len(with_retry) == 19, "expected 19 retry-capable configs; update the sweep"

        missing = [m.__name__ for m in with_retry if "rate_limit" not in m.model_fields]
        assert missing == []

    @pytest.mark.parametrize("cls", _configs_with_retry(), ids=lambda c: c.__name__)
    def test_rate_limit_is_optional_and_defaults_to_none(self, cls: type) -> None:
        """Default None means "no destination override" — sync-level wins."""
        field = cls.model_fields["rate_limit"]
        assert field.default is None
        assert not field.is_required()

    @pytest.mark.parametrize("cls", _configs_with_retry(), ids=lambda c: c.__name__)
    def test_rate_limit_is_typed_as_rate_limit_config(self, cls: type) -> None:
        """A plain dict in YAML must validate into ``RateLimitConfig``."""
        from drt.config.sync_options import RateLimitConfig

        assert cls.model_fields["rate_limit"].annotation == RateLimitConfig | None

    def test_rate_limit_parses_from_yaml_shape(self) -> None:
        """End-to-end: the nested mapping a user writes becomes a config."""
        from drt.config.sync_options import RateLimitConfig

        cfg = RestApiDestinationConfig.model_validate(
            {
                "type": "rest_api",
                "url": "https://api.example.com/v1/users",
                "rate_limit": {"requests_per_second": 2.5, "burst": 4},
            }
        )
        assert isinstance(cfg.rate_limit, RateLimitConfig)
        assert cfg.rate_limit.requests_per_second == 2.5
        assert cfg.rate_limit.burst == 4

    def test_rate_limit_omitted_stays_none(self) -> None:
        cfg = RestApiDestinationConfig.model_validate(
            {"type": "rest_api", "url": "https://api.example.com"}
        )
        assert cfg.rate_limit is None
