"""Docs-safe destination labels (#696).

The docs site (manifest.json + every HTML page) ships each destination's
label; by default that label must never carry a network location or personal
identifier. Three layers of guard here:

1. **Completeness** — every type in the ``DESTINATIONS`` registry has a sample
   config below. Adding a connector without classifying its label breaks the
   build, on purpose: the author must decide what the safe label shows.
2. **Golden labels** — the exact safe label per connector, so a change in
   redaction is a visible, reviewed diff.
3. **Sentinel scan** — every network/personal field in the samples is planted
   with ``LEAKME``; no safe label may contain it. This holds even if the
   golden table is edited carelessly.

Builder-level tests then pin how the labels and node ids reach the manifest.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from drt.config.connectors import DESTINATIONS
from drt.config.destinations_sql import ElasticsearchDestinationConfig
from drt.config.destinations_storage import (
    AzureBlobDestinationConfig,
    GCSDestinationConfig,
    S3DestinationConfig,
)
from drt.config.models import (
    AirtableDestinationConfig,
    AmplitudeDestinationConfig,
    BigQueryDestinationConfig,
    ClickHouseDestinationConfig,
    DatabricksDestinationConfig,
    DiscordDestinationConfig,
    EmailSmtpDestinationConfig,
    FileDestinationConfig,
    GitHubActionsDestinationConfig,
    GoogleAdsDestinationConfig,
    GoogleSheetsDestinationConfig,
    HubSpotDestinationConfig,
    IntercomDestinationConfig,
    JiraDestinationConfig,
    KlaviyoDestinationConfig,
    LinearDestinationConfig,
    MixpanelDestinationConfig,
    MySQLDestinationConfig,
    NotionDestinationConfig,
    ParquetDestinationConfig,
    PostgresDestinationConfig,
    RestApiDestinationConfig,
    SalesforceBulkDestinationConfig,
    SendGridDestinationConfig,
    SlackDestinationConfig,
    SnowflakeDestinationConfig,
    StagedUploadDestinationConfig,
    TeamsDestinationConfig,
    TwilioDestinationConfig,
    ZendeskDestinationConfig,
)

# Planted in every field whose value must NOT surface in a safe label.
S = "LEAKME"

SAMPLES: dict[str, object] = {
    "airtable": AirtableDestinationConfig(
        type="airtable", base_id=f"app{S}", table_name="Leads", api_key_env="K"
    ),
    "amplitude": AmplitudeDestinationConfig(
        type="amplitude", api_key_env="K", endpoint="identify", region="eu"
    ),
    "azure_blob": AzureBlobDestinationConfig(type="azure_blob", container=f"{S}-container"),
    "bigquery": BigQueryDestinationConfig(
        type="bigquery", project="proj", dataset="mart", table="users"
    ),
    "clickhouse": ClickHouseDestinationConfig(
        type="clickhouse", host=f"{S}.corp", database="analytics", table="events"
    ),
    "databricks": DatabricksDestinationConfig(
        type="databricks",
        host_env="H",
        http_path_env="P",
        token_env="T",
        catalog="cat",
        schema="silver",
        table="users",
    ),
    "discord": DiscordDestinationConfig(
        type="discord", webhook_url=f"https://{S}.discord.com/api/webhooks/1"
    ),
    "elasticsearch": ElasticsearchDestinationConfig(
        type="elasticsearch", url=f"https://{S}:9200", index="events"
    ),
    "email_smtp": EmailSmtpDestinationConfig(
        type="email_smtp",
        host=f"smtp.{S}.corp",
        sender=f"ops@{S}.example",
        recipients=[f"x@{S}.example"],
        subject_template="s",
        body_template="b",
    ),
    "file": FileDestinationConfig(type="file", path=f"/home/{S}/exports/users.csv"),
    "gcs": GCSDestinationConfig(type="gcs", bucket=f"{S}-datalake", prefix="crm/"),
    "github_actions": GitHubActionsDestinationConfig(
        type="github_actions", owner=S, repo=S, workflow_id="sync.yml"
    ),
    "google_ads": GoogleAdsDestinationConfig(
        type="google_ads", customer_id=S, conversion_action=f"customers/{S}/x"
    ),
    "google_sheets": GoogleSheetsDestinationConfig(
        type="google_sheets", spreadsheet_id="sheet1", sheet="Leads"
    ),
    "hubspot": HubSpotDestinationConfig(type="hubspot"),
    "intercom": IntercomDestinationConfig(
        type="intercom",
        auth={"type": "bearer", "token_env": "T"},
        properties_template="{}",
    ),
    "jira": JiraDestinationConfig(
        type="jira",
        base_url_env="U",
        email_env="E",
        token_env="T",
        project_key="OPS",
        summary_template="s",
        description_template="d",
    ),
    "klaviyo": KlaviyoDestinationConfig(type="klaviyo", api_key_env="K"),
    "linear": LinearDestinationConfig(type="linear", title_template="t", description_template="d"),
    "mixpanel": MixpanelDestinationConfig(type="mixpanel", token_env="T"),
    "mysql": MySQLDestinationConfig(
        type="mysql", host=f"{S}.corp", dbname="app", table="scores", upsert_key=["id"]
    ),
    "notion": NotionDestinationConfig(type="notion", database_id=f"uuid-{S}", token_env="T"),
    "parquet": ParquetDestinationConfig(type="parquet", path=f"/home/{S}/out/users.parquet"),
    "postgres": PostgresDestinationConfig(
        type="postgres", host=f"{S}.corp", dbname="app", table="public.users", upsert_key=["id"]
    ),
    "rest_api": RestApiDestinationConfig(type="rest_api", url=f"https://{S}.corp/api"),
    "s3": S3DestinationConfig(type="s3", bucket=f"{S}-datalake", prefix="crm/"),
    "salesforce_bulk": SalesforceBulkDestinationConfig(
        type="salesforce_bulk",
        object_name="Contact",
        instance_url=f"https://{S}.my.salesforce.com",
        client_id_env="A",
        client_secret_env="B",
        username_env="C",
        password_env="D",
    ),
    "sendgrid": SendGridDestinationConfig(
        type="sendgrid",
        from_email=f"{S}@corp.example",
        subject_template="s",
        body_template="b",
    ),
    "slack": SlackDestinationConfig(type="slack", webhook_url=f"https://{S}.slack.com/x"),
    "snowflake": SnowflakeDestinationConfig(
        type="snowflake",
        account_env="A",
        user_env="U",
        password_env="P",
        database="DB",
        schema="PUBLIC",
        table="USERS",
        warehouse="WH",
    ),
    "staged_upload": StagedUploadDestinationConfig(
        type="staged_upload",
        stage={"url": f"https://{S}.corp/upload"},
        trigger={"url": f"https://{S}.corp/run"},
    ),
    "teams": TeamsDestinationConfig(type="teams", webhook_url=f"https://{S}.webhook.office.com/x"),
    "twilio": TwilioDestinationConfig(
        type="twilio",
        account_sid_env="SID",
        auth_token_env="TOK",
        from_number="+815550100",
        to_template="{{ p }}",
        message_template="m",
    ),
    "zendesk": ZendeskDestinationConfig(type="zendesk"),
}

# The reviewed redaction policy, one line per connector. Object identity
# (table / channel / sheet / bucket / object type) stays; network locations
# and personal identifiers do not.
EXPECTED_SAFE: dict[str, str] = {
    "airtable": "airtable (Leads)",  # base id dropped
    "amplitude": "amplitude",  # endpoint/region dropped
    "azure_blob": "azure_blob",  # container dropped (probeable); no prefix -> type-only
    "bigquery": "bigquery (proj.mart.users)",
    "clickhouse": "clickhouse (events)",
    "databricks": "databricks (cat.silver.users)",
    "discord": "discord (webhook)",
    "elasticsearch": "elasticsearch (events)",  # url dropped by describe() already
    "email_smtp": "email_smtp",  # host dropped
    "file": "file (users.csv)",  # directory dropped (can carry a home dir)
    "gcs": "gcs (crm/)",  # bucket dropped, per-sync routing prefix kept
    "github_actions": "github_actions",  # owner/repo dropped (private repo names)
    "google_ads": "google_ads",  # customer id dropped
    "google_sheets": "google_sheets (Leads)",
    "hubspot": "hubspot (contacts)",
    "intercom": "intercom (contacts)",
    "jira": "jira (OPS)",
    "klaviyo": "klaviyo (profiles)",
    "linear": "linear (issue)",
    "mixpanel": "mixpanel",  # endpoint/region dropped
    "mysql": "mysql (scores)",
    "notion": "notion (database)",  # database uuid dropped
    "parquet": "parquet (users.parquet)",  # directory dropped
    "postgres": "postgres (public.users)",
    "rest_api": "rest_api",  # full endpoint URL dropped
    "s3": "s3 (crm/)",  # bucket dropped, per-sync routing prefix kept
    "salesforce_bulk": "salesforce_bulk (Contact)",
    "sendgrid": "sendgrid (…@corp.example)",  # local part masked, org domain kept
    "slack": "slack (webhook)",
    "snowflake": "snowflake (DB.PUBLIC.USERS)",
    "staged_upload": "staged_upload",
    "teams": "teams (webhook)",
    "twilio": "twilio (+81…)",  # country-code prefix only
    "zendesk": "zendesk (user)",
}


def test_every_registered_destination_is_classified() -> None:
    """A new connector must land here with a reviewed safe label (#696)."""
    registered = {t for t, _ in DESTINATIONS}
    assert set(SAMPLES) == registered, (
        "SAMPLES/EXPECTED_SAFE out of sync with the DESTINATIONS registry — "
        "classify the new connector's docs label (see module docstring)."
    )
    assert set(EXPECTED_SAFE) == registered


@pytest.mark.parametrize("dest_type", sorted(SAMPLES))
def test_safe_label_matches_golden(dest_type: str) -> None:
    dest = SAMPLES[dest_type]
    safe = getattr(dest, "describe_safe", None)
    label = safe() if callable(safe) else str(dest.type)
    assert label == EXPECTED_SAFE[dest_type]


@pytest.mark.parametrize("dest_type", sorted(SAMPLES))
def test_safe_label_never_leaks_planted_sentinel(dest_type: str) -> None:
    """Structural guard, independent of the golden table's correctness."""
    dest = SAMPLES[dest_type]
    safe = getattr(dest, "describe_safe", None)
    label = safe() if callable(safe) else str(dest.type)
    assert S not in label, f"{dest_type}: safe label leaks a sensitive field: {label!r}"


# ---------------------------------------------------------------------------
# Builder integration (#696): labels + node ids in the manifest
# ---------------------------------------------------------------------------


def _write_project(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: demo\nprofile: default\n")
    (tmp_path / "syncs").mkdir(exist_ok=True)


def _write_rest_sync(tmp_path: Path, name: str, url: str) -> None:
    (tmp_path / "syncs" / f"{name}.yml").write_text(
        yaml.dump(
            {"name": name, "model": "SELECT 1", "destination": {"type": "rest_api", "url": url}}
        )
    )


def test_manifest_labels_are_safe_by_default(tmp_path: Path) -> None:
    from drt.docs.builder import build_manifest

    _write_project(tmp_path)
    _write_rest_sync(tmp_path, "push", "https://internal-host.corp/api")

    manifest = build_manifest(tmp_path)

    (dest,) = manifest.destinations
    assert dest.label == "rest_api"
    assert "internal-host" not in str(manifest.to_dict())


def test_full_labels_restores_verbatim_describe(tmp_path: Path) -> None:
    from drt.docs.builder import build_manifest

    _write_project(tmp_path)
    _write_rest_sync(tmp_path, "push", "https://internal-host.corp/api")

    manifest = build_manifest(tmp_path, full_labels=True)

    (dest,) = manifest.destinations
    assert dest.label == "rest_api (https://internal-host.corp/api)"


def test_destination_ids_stay_distinct_when_safe_labels_collide(tmp_path: Path) -> None:
    """Two rest_api endpoints both label as 'rest_api' — the hashed node id
    must keep them apart (separate pages, separate DAG nodes)."""
    from drt.docs.builder import build_manifest

    _write_project(tmp_path)
    _write_rest_sync(tmp_path, "push_a", "https://a.corp/api")
    _write_rest_sync(tmp_path, "push_b", "https://b.corp/api")

    manifest = build_manifest(tmp_path)

    assert len(manifest.destinations) == 2
    names = sorted(d.name for d in manifest.destinations)
    # Ids derive from the SAFE label + a deterministic counter — never from
    # the endpoint. (The first cut hashed the unredacted describe(); review
    # showed a truncated hash of a low-entropy value is brute-forceable, so
    # no function of the sensitive string may ship at all.)
    assert names == ["dest_rest_api", "dest_rest_api_2"]
    for n in names:
        assert "corp" not in n and "http" not in n


def test_destination_ids_are_deterministic(tmp_path: Path) -> None:
    from drt.docs.builder import build_manifest

    _write_project(tmp_path)
    _write_rest_sync(tmp_path, "push", "https://a.corp/api")

    first = {d.name for d in build_manifest(tmp_path).destinations}
    second = {d.name for d in build_manifest(tmp_path).destinations}
    assert first == second


def test_destination_ids_survive_sync_file_renames(tmp_path: Path) -> None:
    """Renaming a sync FILE must not swap which destination owns which id —
    a bookmarked destination page would silently show the other endpoint's
    syncs (#805 review, @Pawansingh3889). Suffix rank follows the referencing
    sync *name* (manifest-public, file-independent), not file order."""
    from drt.docs.builder import build_manifest

    _write_project(tmp_path)
    _write_rest_sync(tmp_path, "alerts", "https://a.corp/api")
    _write_rest_sync(tmp_path, "digests", "https://b.corp/api")

    def id_of(sync_name: str, manifest) -> str:
        (sync,) = [s for s in manifest.syncs if s.name == sync_name]
        return sync.destination

    before = build_manifest(tmp_path)
    # ファイル名だけ変える(sync 名はそのまま)
    (tmp_path / "syncs" / "alerts.yml").rename(tmp_path / "syncs" / "zz_renamed.yml")
    after = build_manifest(tmp_path)

    assert id_of("alerts", before) == id_of("alerts", after)
    assert id_of("digests", before) == id_of("digests", after)


def test_destination_ids_do_not_depend_on_label_mode(tmp_path: Path) -> None:
    """--full-labels changes labels only — ids (page filenames, graph nodes)
    must not be rewired, and must stay non-identifying in both modes."""
    from drt.docs.builder import build_manifest

    _write_project(tmp_path)
    _write_rest_sync(tmp_path, "push", "https://internal-host.corp/api")

    safe_ids = {d.name for d in build_manifest(tmp_path).destinations}
    full_ids = {d.name for d in build_manifest(tmp_path, full_labels=True).destinations}

    assert safe_ids == full_ids == {"dest_rest_api"}
