"""SaaS / API / messaging destination configs (#721 split from models.py).

Webhook, CRM, marketing, issue-tracker, email and staged-upload destinations.
All ``*DestinationConfig`` here are members of the
:data:`~drt.config.sync_options.DestinationConfig` union.
"""

from __future__ import annotations

from typing import Literal
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator, model_validator

from drt.config.base import (
    LITERAL_CREDENTIAL_KEY,
    ApiKeyAuth,
    AuthConfig,
    BasicAuth,
    BearerAuth,
    DescribableConfig,
    OAuth2ClientCredentialsAuth,
    PaginationConfig,
    RateLimitConfig,
    RetryConfig,
)


def _webhook_identity(url: str | None, url_env: str | None) -> str:
    """Identify a webhook endpoint for rate-limit keying (#769).

    Shared by Slack / Discord / Teams, whose configs are the same shape and
    whose ``describe()`` is the literal ``"webhook"`` for every instance. The
    env-var name wins when present; a literal URL is itself the credential
    (anyone holding it can post) so only its digest goes in the key.
    """
    if url_env:
        return url_env
    if url:
        return LITERAL_CREDENTIAL_KEY
    return "unset"


def _auth_identity(auth: AuthConfig | None) -> str:
    """Identify the account behind an auth block for rate-limit keying (#769).

    Env-var *names* are preferred over the secrets they hold: ``token_env``
    pins the same account just as reliably, and keeps the credential out of a
    dict key entirely. A literal is hashed. Configs sharing one credential
    share one vendor quota, which is exactly the bucketing the registry wants.
    """
    if auth is None:
        return "noauth"
    if isinstance(auth, BearerAuth):
        return auth.token_env or (LITERAL_CREDENTIAL_KEY if auth.token else "noauth")
    if isinstance(auth, ApiKeyAuth):
        return auth.value_env or (LITERAL_CREDENTIAL_KEY if auth.value else "noauth")
    if isinstance(auth, BasicAuth):
        return auth.username_env
    if isinstance(auth, OAuth2ClientCredentialsAuth):
        # The token URL is the tenant; client_id_env disambiguates apps on it.
        return f"{auth.token_url}|{auth.client_id_env}"
    return "noauth"  # pragma: no cover - AuthConfig union is exhaustive above


class RestApiDestinationConfig(DescribableConfig):
    type: Literal["rest_api"]
    url: str
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body_template: str | None = None
    auth: AuthConfig | None = None
    pagination: PaginationConfig | None = None
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return f"{self.url}"

    def rate_limit_key(self) -> str:
        """Per host (#769). Paths vary sync to sync — ``/v1/users`` and
        ``/v2/orders`` on one API — but the published quota is per host, so
        ``netloc`` (host plus any port) is the endpoint that shares it. This is
        also what makes ``rest_api.py``'s two limiters resolve to one bucket.
        """
        return f"{self.type}:{urlparse(self.url).netloc}"


class SlackDestinationConfig(DescribableConfig):
    _detail_is_public = True  # object identity only (#696) — safe for hosted docs
    type: Literal["slack"]
    webhook_url: str | None = None
    webhook_url_env: str | None = None
    # Jinja2 template for Slack message. Supports plain text or Block Kit JSON.
    # Plain text example: "New user: {{ row.name }} ({{ row.email }})"
    # Block Kit: full JSON payload as template string
    message_template: str = "{{ row }}"
    # If True, treat message_template as a Block Kit JSON payload
    block_kit: bool = False
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return "webhook"

    def rate_limit_key(self) -> str:
        """Per webhook (#769). ``describe()`` returns the literal ``"webhook"``
        for every Slack config, so keying on it would throttle unrelated
        workspaces against each other. The env-var name is preferred; a literal
        URL is a credential and is hashed rather than embedded.
        """
        return f"{self.type}:{_webhook_identity(self.webhook_url, self.webhook_url_env)}"


class TwilioDestinationConfig(DescribableConfig):
    type: Literal["twilio"]

    account_sid: str | None = None
    account_sid_env: str | None = None

    auth_token: str | None = None
    auth_token_env: str | None = None

    from_number: str  # Twilio phone number (E.164 format)

    # Jinja2 template → destination phone number
    to_template: str  # e.g. "{{ row.phone }}"

    # Jinja2 template → SMS body
    message_template: str
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return f"{self.from_number}"

    def describe_safe(self) -> str:
        # Country-code prefix only — enough to tell regions apart, not a number.
        prefix = self.from_number[:3] if self.from_number.startswith("+") else ""
        return f"{self.type} ({prefix}\u2026)" if prefix else str(self.type)

    def rate_limit_key(self) -> str:
        """Per Twilio account (#769) \u2014 the quota follows the account SID, not
        the sending number. Note ``describe_safe()`` deliberately keeps only a
        country-code prefix (#696); that lossiness is why it cannot be the key.
        """
        account = self.account_sid_env or (
            LITERAL_CREDENTIAL_KEY if self.account_sid else "unset"
        )
        return f"{self.type}:{account}"

    @model_validator(mode="after")
    def _check_auth(self) -> TwilioDestinationConfig:
        if not (self.account_sid or self.account_sid_env):
            raise ValueError("account_sid or account_sid_env is required.")
        if not (self.auth_token or self.auth_token_env):
            raise ValueError("auth_token or auth_token_env is required.")
        return self


class DiscordDestinationConfig(DescribableConfig):
    _detail_is_public = True  # object identity only (#696) — safe for hosted docs
    type: Literal["discord"]
    webhook_url: str | None = None
    webhook_url_env: str | None = None
    # Jinja2 template for Discord message. Supports plain text or embeds JSON.
    # Plain text example: "New user: {{ row.name }} ({{ row.email }})"
    # Embeds: full JSON payload with "embeds" array
    message_template: str = "{{ row }}"
    # If True, treat message_template as a JSON payload with embeds
    embeds: bool = False
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return "webhook"

    def rate_limit_key(self) -> str:
        """Per webhook (#769) — Discord rate-limits each webhook route
        separately, and ``describe()`` collides them all as ``"webhook"``.
        """
        return f"{self.type}:{_webhook_identity(self.webhook_url, self.webhook_url_env)}"


class GitHubActionsDestinationConfig(DescribableConfig):
    type: Literal["github_actions"]
    owner: str
    repo: str
    workflow_id: str  # filename (e.g. "deploy.yml") or workflow ID
    ref: str = "main"  # branch/tag to run on
    # Jinja2 template → JSON object for workflow inputs
    # Example: '{"environment": "{{ row.env }}", "version": "{{ row.version }}"}'
    inputs_template: str | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return f"{self.owner}/{self.repo}"

    def rate_limit_key(self) -> str:
        """Per GitHub token (#769). GitHub's REST quota is per authenticated
        user / app across all repositories, so two syncs dispatching workflows
        in different repos under one token still share one budget — keying on
        ``owner/repo`` would split a quota that is not actually split.
        """
        return f"{self.type}:{_auth_identity(self.auth)}"


class GoogleSheetsDestinationConfig(DescribableConfig):
    _detail_is_public = True  # object identity only (#696) — safe for hosted docs
    type: Literal["google_sheets"]
    spreadsheet_id: str
    sheet: str = "Sheet1"
    mode: Literal["overwrite", "append"] = "overwrite"
    credentials_path: str | None = None
    credentials_env: str | None = None

    def _describe_detail(self) -> str:
        return f"{self.sheet}"


class HubSpotDestinationConfig(DescribableConfig):
    _detail_is_public = True  # object identity only (#696) — safe for hosted docs
    type: Literal["hubspot"]
    object_type: Literal["contacts", "deals", "companies"] = "contacts"
    # Property used as upsert key (contacts → email, deals → dealname, etc.)
    id_property: str = "email"
    # Jinja2 template → JSON object of HubSpot properties
    # Example: '{"email": "{{ row.email }}", "firstname": "{{ row.name }}"}'
    properties_template: str | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return f"{self.object_type}"

    def rate_limit_key(self) -> str:
        """Per portal, i.e. per token (#769).

        ``object_type`` must NOT appear here. HubSpot's quota is per portal and
        shared across contacts / deals / companies, so keying on
        ``describe()`` — which *does* include ``object_type`` — would hand
        ``hubspot (contacts)`` and ``hubspot (deals)`` a limiter each and let
        one portal be hit at twice the configured rate. That under-sharing is
        the exact bug this key exists to prevent.
        """
        return f"{self.type}:{_auth_identity(self.auth)}"


class ZendeskDestinationConfig(DescribableConfig):
    _detail_is_public = True  # object identity only (#696) — safe for hosted docs
    type: Literal["zendesk"]
    subdomain: str | None = None
    subdomain_env: str | None = None
    email: str | None = None
    email_env: str | None = None
    api_token: str | None = None
    api_token_env: str | None = None
    object: Literal["user", "organization"] = "user"
    id_field: str | None = None
    custom_fields_template: str | None = None
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return f"{self.object}"

    def rate_limit_key(self) -> str:
        """Per subdomain (#769) — one Zendesk instance, one quota, whichever
        object a sync writes. ``describe()`` details the object instead, so
        users and organizations on one subdomain would wrongly get a bucket each.
        """
        subdomain = self.subdomain_env or self.subdomain or "unset"
        return f"{self.type}:{subdomain}"


class AmplitudeDestinationConfig(DescribableConfig):
    type: Literal["amplitude"]
    api_key: str | None = None
    api_key_env: str | None = "AMPLITUDE_API_KEY"
    region: Literal["default", "eu"] = "default"
    endpoint: Literal["identify", "event"] = "identify"
    user_id_field: str = "user_id"
    device_id_field: str | None = None
    event_type_field: str | None = None
    event_type: str | None = None
    time_field: str | None = None
    insert_id_field: str | None = None
    properties_template: str | None = None
    batch_size: int = 1000
    min_id_length: int | None = None
    retry: RetryConfig | None = None
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return f"{self.endpoint}, {self.region}"

    def rate_limit_key(self) -> str:
        """Per project (API key) and region (#769). The two endpoints
        (``identify`` / ``event``) bill against one project quota, so the
        endpoint is excluded — but ``region`` is a genuinely different host
        (``api.eu`` vs the default) with its own budget, so it stays.
        """
        account = self.api_key_env or (LITERAL_CREDENTIAL_KEY if self.api_key else "unset")
        return f"{self.type}:{self.region}:{account}"

    @model_validator(mode="after")
    def _check_api_key(self) -> AmplitudeDestinationConfig:
        if not self.api_key and not self.api_key_env:
            raise ValueError("api_key or api_key_env is required.")
        return self

    @model_validator(mode="after")
    def _check_event_endpoint(self) -> AmplitudeDestinationConfig:
        if self.endpoint == "event" and not self.event_type and not self.event_type_field:
            raise ValueError("event_type or event_type_field is required when endpoint is 'event'.")
        return self

    @field_validator("batch_size", mode="after")
    @classmethod
    def _clamp_batch_size(cls, value: int) -> int:
        return max(1, min(value, 1000))


class AirtableDestinationConfig(BaseModel):
    type: Literal["airtable"]
    access_token: str | None = None
    access_token_env: str | None = "AIRTABLE_TOKEN"
    base_id: str
    table_name: str
    # When set, records are upserted by matching this field (Airtable
    # performUpsert / fieldsToMergeOn). Omit for append-only.
    primary_key: str | None = None
    retry: RetryConfig | None = None
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def describe(self) -> str:
        return f"airtable ({self.base_id}/{self.table_name})"

    def describe_safe(self) -> str:
        return f"airtable ({self.table_name})"

    def rate_limit_key(self) -> str:
        """Per base (#769) — Airtable's 5 req/s cap is per base, shared by every
        table in it, so ``table_name`` is deliberately excluded.

        Defined here rather than inherited: this config subclasses ``BaseModel``
        directly (it hand-rolls ``describe()``), one of ten SaaS configs that do.
        Normalizing them onto ``DescribableConfig`` is a separate refactor.
        """
        return f"{self.type}:{self.base_id}"

    @model_validator(mode="after")
    def _check_token(self) -> AirtableDestinationConfig:
        if not self.access_token and not self.access_token_env:
            raise ValueError("access_token or access_token_env is required.")
        return self


class KlaviyoDestinationConfig(BaseModel):
    type: Literal["klaviyo"]
    api_key: str | None = None
    api_key_env: str | None = "KLAVIYO_API_KEY"
    # Row field used as the profile identifier (email).
    email_field: str = "email"
    # Jinja2 JSON template → custom profile properties. When omitted, all
    # row fields except email_field are sent as custom properties.
    properties_template: str | None = None
    # Optional: add each upserted profile to this Klaviyo list.
    list_id: str | None = None
    list_id_env: str | None = None
    # Klaviyo API revision (sent as the `revision` header).
    revision: str = "2024-10-15"
    retry: RetryConfig | None = None
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def describe(self) -> str:
        return "klaviyo (profiles)"

    def describe_safe(self) -> str:
        return self.describe()  # detail is object identity only (#696)

    def rate_limit_key(self) -> str:
        """Per Klaviyo account (API key) (#769). ``list_id`` is excluded — the
        quota is account-wide, and ``describe()`` is the constant
        ``"klaviyo (profiles)"`` for every config, so it cannot serve as a key.
        (``BaseModel``-direct config — see ``AirtableDestinationConfig``.)
        """
        account = self.api_key_env or (LITERAL_CREDENTIAL_KEY if self.api_key else "unset")
        return f"{self.type}:{account}"

    @model_validator(mode="after")
    def _check_api_key(self) -> KlaviyoDestinationConfig:
        if not self.api_key and not self.api_key_env:
            raise ValueError("api_key or api_key_env is required.")
        return self


class MixpanelDestinationConfig(DescribableConfig):
    type: Literal["mixpanel"]
    # endpoint selects which Mixpanel API to target:
    #   people_set    -> /engage#profile-set (auth: project token, per-record)
    #   import_events -> /import             (auth: service account + project_id)
    endpoint: Literal["people_set", "import_events"] = "people_set"
    region: Literal["default", "eu"] = "default"

    # people_set auth — project token
    project_token: str | None = None
    project_token_env: str | None = "MIXPANEL_TOKEN"

    # import_events auth — service account + numeric project id
    project_id: str | None = None
    service_account_username: str | None = None
    service_account_username_env: str | None = "MIXPANEL_SA_USERNAME"
    service_account_secret: str | None = None
    service_account_secret_env: str | None = "MIXPANEL_SA_SECRET"

    # field mapping
    distinct_id_field: str = "distinct_id"
    event_name_field: str | None = None  # import_events: per-row event name
    event_name: str | None = None  # import_events: constant event name
    time_field: str | None = None  # import_events: row field for event time
    insert_id_field: str | None = None  # import_events: dedup id (else derived)
    properties_template: str | None = None

    batch_size: int = 2000
    retry: RetryConfig | None = None
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return f"{self.endpoint}, {self.region}"

    def rate_limit_key(self) -> str:
        """Per Mixpanel project and region (#769).

        The two endpoints authenticate differently — ``people_set`` with a
        project token, ``import_events`` with a service account plus numeric
        project id — so the identity is taken from whichever applies. Both bill
        against one project, so the endpoint itself is not part of the key.
        """
        if self.endpoint == "people_set":
            account = self.project_token_env or (
                LITERAL_CREDENTIAL_KEY if self.project_token else "unset"
            )
        else:
            account = self.project_id or "unset"
        return f"{self.type}:{self.region}:{account}"

    @model_validator(mode="after")
    def _check_auth(self) -> MixpanelDestinationConfig:
        if self.endpoint == "people_set":
            if not self.project_token and not self.project_token_env:
                raise ValueError(
                    "project_token or project_token_env is required for endpoint 'people_set'."
                )
        else:  # import_events
            if not self.project_id:
                raise ValueError("project_id is required for endpoint 'import_events'.")
            has_user = self.service_account_username or self.service_account_username_env
            has_secret = self.service_account_secret or self.service_account_secret_env
            if not has_user or not has_secret:
                raise ValueError(
                    "service account credentials (username + secret, or their *_env "
                    "vars) are required for endpoint 'import_events'."
                )
        return self

    @model_validator(mode="after")
    def _check_event_name(self) -> MixpanelDestinationConfig:
        if self.endpoint == "import_events" and not self.event_name and not self.event_name_field:
            raise ValueError(
                "event_name or event_name_field is required when endpoint is 'import_events'."
            )
        return self

    @field_validator("batch_size", mode="after")
    @classmethod
    def _clamp_batch_size(cls, value: int) -> int:
        # Mixpanel caps both /engage and /import at 2000 records per request.
        return max(1, min(value, 2000))


class IntercomDestinationConfig(DescribableConfig):
    _detail_is_public = True  # object identity only (#696) — safe for hosted docs
    type: Literal["intercom"]

    auth: AuthConfig

    # Jinja2 JSON template for contact payload
    properties_template: str

    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return "contacts"

    def rate_limit_key(self) -> str:
        """Per Intercom workspace, identified by its access token (#769).
        ``describe()`` is the constant ``"contacts"`` for every config, so it
        would collide unrelated workspaces into one bucket.
        """
        return f"{self.type}:{_auth_identity(self.auth)}"


class SendGridDestinationConfig(BaseModel):
    type: Literal["sendgrid"]
    from_email: str
    from_name: str | None = None
    subject_template: str
    body_template: str
    to_email_field: str = "email"
    list_ids: list[str] | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def describe(self) -> str:
        return f"sendgrid ({self.from_email})"

    def describe_safe(self) -> str:
        domain = self.from_email.rsplit("@", 1)[-1] if "@" in self.from_email else ""
        return f"sendgrid (\u2026@{domain})" if domain else "sendgrid"

    def rate_limit_key(self) -> str:
        """Per SendGrid API key (#769). The sending quota belongs to the key,
        not the From address, so two syncs sending as different senders on one
        key still share the budget. (``BaseModel``-direct config.)
        """
        return f"{self.type}:{_auth_identity(self.auth)}"


class LinearDestinationConfig(BaseModel):
    type: Literal["linear"]
    team_id: str | None = None
    team_id_env: str | None = None
    title_template: str
    description_template: str
    label_ids: list[str] = []
    assignee_id: str | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def describe(self) -> str:
        return "linear (issue)"

    def describe_safe(self) -> str:
        return self.describe()  # detail is object identity only (#696)

    def rate_limit_key(self) -> str:
        """Per Linear API key (#769) — the GraphQL quota is per authenticated
        actor across teams, so ``team_id`` is excluded. ``describe()`` is the
        constant ``"linear (issue)"``. (``BaseModel``-direct config.)
        """
        return f"{self.type}:{_auth_identity(self.auth)}"


class TeamsDestinationConfig(DescribableConfig):
    _detail_is_public = True  # object identity only (#696) — safe for hosted docs
    type: Literal["teams"]
    webhook_url: str | None = None
    webhook_url_env: str | None = None
    # Jinja2 template for the message card. Supports plain text or Adaptive Card JSON.
    message_template: str = "{{ row }}"
    # If True, treat message_template as an Adaptive Card JSON payload
    adaptive_card: bool = False
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return "webhook"

    def rate_limit_key(self) -> str:
        """Per webhook (#769) — same shape and same ``describe()`` collision as
        Slack and Discord.
        """
        return f"{self.type}:{_webhook_identity(self.webhook_url, self.webhook_url_env)}"


class JiraDestinationConfig(BaseModel):
    type: Literal["jira"]
    base_url_env: str  # e.g. JIRA_BASE_URL -> https://myorg.atlassian.net
    email_env: str  # Jira account email env var
    token_env: str  # Jira API token env var
    project_key: str  # can include Jinja2 template syntax
    issue_type: str = "Task"  # can include Jinja2 template syntax
    summary_template: str
    description_template: str
    issue_id_field: str = "issue_id"  # row key that indicates update mode
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def describe(self) -> str:
        return f"jira ({self.project_key})"

    def describe_safe(self) -> str:
        return self.describe()  # detail is object identity only (#696)

    def rate_limit_key(self) -> str:
        """Per Jira site (#769) — one Atlassian site shares a quota across all
        its projects, so ``project_key`` is excluded (it may even be a Jinja2
        template, and a key must be resolvable without rendering). Both env-var
        names are already names rather than secrets. (``BaseModel``-direct config.)
        """
        return f"{self.type}:{self.base_url_env}:{self.token_env}"


class EmailSmtpDestinationConfig(DescribableConfig):
    type: Literal["email_smtp"] = "email_smtp"
    host: str
    port: int = 587
    sender: str
    recipients: list[str]
    subject_template: str
    body_template: str
    use_tls: bool = True
    username: str | None = None
    username_env: str | None = None
    password: str | None = None
    password_env: str | None = None

    def _describe_detail(self) -> str:
        return f"{self.host}"

    def rate_limit_key(self) -> str:
        """Per SMTP server (#769). Relays throttle per host (and per port, since
        submission and SMTPS are separate services), not per sender.
        """
        return f"{self.type}:{self.host}:{self.port}"


class NotionDestinationConfig(DescribableConfig):
    type: Literal["notion"]
    database_id: str
    # Jinja2 template → JSON object of Notion page properties
    # Example: see https://developers.notion.com/reference/post-page for template format
    properties_template: str | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def _describe_detail(self) -> str:
        return f"database {self.database_id}"

    def describe_safe(self) -> str:
        return f"{self.type} (database)"

    def rate_limit_key(self) -> str:
        """Per integration token (#769). Notion's documented ~3 req/s average is
        enforced per integration across every database it can reach, so
        ``database_id`` is excluded — two syncs writing two databases through one
        integration must share the bucket or they will together exceed the cap.
        """
        return f"{self.type}:{_auth_identity(self.auth)}"


class GoogleAdsDestinationConfig(BaseModel):
    type: Literal["google_ads"]
    customer_id: str  # Google Ads customer ID (without hyphens)
    conversion_action: str  # e.g. "customers/123/conversionActions/456"
    gclid_field: str = "gclid"  # row field containing the click ID
    conversion_time_field: str = "conversion_time"  # row field for timestamp
    conversion_value_field: str | None = None  # optional: row field for value
    currency_code: str = "USD"
    developer_token_env: str = "GOOGLE_ADS_DEVELOPER_TOKEN"
    auth: AuthConfig | None = None  # typically oauth2_client_credentials
    retry: RetryConfig | None = None  # destination-level override of sync.retry
    rate_limit: RateLimitConfig | None = None  # destination-level override of sync.rate_limit

    def describe(self) -> str:
        return f"google_ads ({self.customer_id})"

    def describe_safe(self) -> str:
        return "google_ads"

    def rate_limit_key(self) -> str:
        """Per developer token (#769). Google Ads meters API operations against
        the developer token, so accounts sharing one token share the budget;
        ``customer_id`` is the narrower scope and would over-split it.
        ``describe_safe()`` drops the id entirely (#696), another reason it
        cannot be the key. (``BaseModel``-direct config.)
        """
        return f"{self.type}:{self.developer_token_env}"


class StagedUploadPhaseConfig(BaseModel):
    url: str
    method: str = "POST"
    headers: dict[str, str] | None = None
    auth: AuthConfig | None = None
    body_template: str | None = None
    response_extract: dict[str, str] | None = None


class StagedUploadPollConfig(BaseModel):
    url: str
    method: str = "GET"
    headers: dict[str, str] | None = None
    auth: AuthConfig | None = None
    status_field: str = "status"
    success_values: list[str] = ["SUCCEEDED", "COMPLETED"]
    failure_values: list[str] = ["FAILED", "ERROR"]
    interval_seconds: int = 30
    timeout_seconds: int = 3600


class StagedUploadDestinationConfig(BaseModel):
    type: Literal["staged_upload"]
    stage: StagedUploadPhaseConfig
    trigger: StagedUploadPhaseConfig
    poll: StagedUploadPollConfig | None = None
    format: Literal["csv", "json", "jsonl"] = "csv"

    def describe(self) -> str:
        return "staged_upload"

    def describe_safe(self) -> str:
        return self.describe()  # detail is object identity only (#696)

    def rate_limit_key(self) -> str:
        """Per trigger host (#769). A staged upload talks to several URLs, but
        the stage step is usually a one-shot presigned URL on a storage host
        while the trigger is the vendor API that actually publishes a quota — so
        the trigger's ``netloc`` is the endpoint worth pacing.
        ``describe()`` is the bare constant ``"staged_upload"``.
        (``BaseModel``-direct config.)
        """
        return f"{self.type}:{urlparse(self.trigger.url).netloc}"


class SalesforceBulkDestinationConfig(BaseModel):
    type: Literal["salesforce_bulk"]
    instance_url: str | None = None
    instance_url_env: str | None = None
    object_name: str  # e.g. "Contact", "Account"
    operation: Literal["insert", "update", "upsert", "delete"] = "upsert"
    external_id_field: str = "Id"
    poll_timeout_seconds: int = 3600
    poll_interval_seconds: int = 30
    client_id_env: str
    client_secret_env: str
    username_env: str
    password_env: str

    def describe(self) -> str:
        return f"salesforce_bulk ({self.object_name})"

    def describe_safe(self) -> str:
        return self.describe()  # detail is object identity only (#696)

    def rate_limit_key(self) -> str:
        """Per Salesforce org (#769). The daily API allocation is org-wide, so
        ``object_name`` — which ``describe()`` details — is excluded: loading
        Contacts and Accounts into one org draws on one budget. The instance is
        identified by its env-var name where given. (``BaseModel``-direct config.)
        """
        org = self.instance_url_env or (
            LITERAL_CREDENTIAL_KEY if self.instance_url else "unset"
        )
        return f"{self.type}:{org}"

    @model_validator(mode="after")
    def _check_instance_url(self) -> SalesforceBulkDestinationConfig:
        if not self.instance_url and not self.instance_url_env:
            raise ValueError("Either instance_url or instance_url_env is required.")
        return self
