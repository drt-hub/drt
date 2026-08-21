# Secret Provider URIs — Resolve Credentials from a Managed Secret Store

Production reverse ETL concentrates *write* credentials to CRMs, ad
platforms, and warehouses — exactly the secrets an org keeps in AWS
Secrets Manager, GCP Secret Manager, or Vault, with rotation policies
attached. The default path (export into an env var in every runner — CI
YAML, Airflow connections, cron wrappers) duplicates secret-distribution
logic per orchestrator and breaks rotation the moment it's exported: the
env snapshot goes stale even though the store itself rotated cleanly.

A `*_env` field in a profile or destination config can be a
`scheme://...` URI instead of a plain env var name, on connectors that
route credential resolution through `resolve_env()`
(`drt/config/credentials.py`). drt resolves it from the matching
provider at connection time — no wrapper script, no re-export step.

## Quick Start

```yaml
# ~/.drt/profiles.yml
prod:
  type: snowflake
  account_env: SNOWFLAKE_ACCOUNT
  user_env: SNOWFLAKE_USER
  password_env: "aws-sm://prod/drt/snowflake#password"
```

`account_env` and `user_env` above are still plain env var names — nothing
about this feature requires every credential in a profile to go through a
provider. Mix and match per field.

## Resolution chain

```
explicit YAML value  >  OS environment variable  >  .drt/secrets.toml  >  provider URI
```

The four steps are tried in order and the first that resolves wins — see
`docs/llm/API_REFERENCE.md`'s `.drt/secrets.toml` section for the first
three. A provider URI only gets tried once the other three have all come
back empty, which is automatic: `aws-sm://prod/drt/snowflake#password`
doesn't collide with a real env var name or a `secrets.toml` key, so it
falls through unchanged.

The connector audit for [#965](https://github.com/drt-hub/drt/issues/965)
fixed direct environment access in the `discord`, `email_smtp`,
`google_ads`, `google_sheets`, `intercom`, `jira`, `salesforce_bulk`,
`slack`, `teams`, and `twilio` destinations and the `postgres` and
`redshift` sources. These now accept provider URIs on their credential
`*_env` fields just like the connectors that already used `resolve_env()`.
On an empty destination batch, credential resolution is skipped entirely,
so a provider URI does not cause a needless Secrets Manager or Vault request.

## Providers

### AWS Secrets Manager — `aws-sm://`

```
aws-sm://<secret-id-or-arn>#<key>
```

- `<secret-id-or-arn>` is whatever `GetSecretValue` accepts — a plain
  secret name (`prod/drt/snowflake`) or a full ARN.
- `#<key>` is optional. Omit it when the secret **is** the value (a
  single password stored as a plain string). Include it to select one
  field out of a secret stored as a JSON object holding several related
  values under one id — `aws-sm://prod/drt/snowflake#password` alongside
  a sibling `#username` in the same secret, say.
- Requires: `pip install drt-core[aws-secrets]`
- Auth: boto3's own default credential chain (env vars,
  `~/.aws/credentials`, an attached IAM role) — nothing drt-specific to
  configure.

Minimal IAM policy for a secret (or a prefix of them) drt should be able
to read:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:*:*:secret:prod/drt/*"
    }
  ]
}
```

### GCP Secret Manager — `gcp-sm://`

```
gcp-sm://projects/<project>/secrets/<secret>/versions/<version-or-latest>#<key>
```

- The path is the **full resource name** `AccessSecretVersion` accepts —
  unlike AWS, GCP always names a version explicitly, with `latest` as a
  real, resolvable alias rather than an implicit default. There's no
  shorter form.
- `#<key>` works the same as the AWS leg — optional, selects a field out
  of a JSON-object-valued secret.
- Requires: `pip install drt-core[gcp-secrets]`
- Auth: Application Default Credentials (`gcloud auth
  application-default login` locally; the attached service account in
  CI/production) — nothing drt-specific to configure.

Grant the built-in `roles/secretmanager.secretAccessor` role (or, for a
narrower custom role, just the `secretmanager.versions.access`
permission) on the secret or the project:

```bash
gcloud secrets add-iam-policy-binding drt-sf \
  --member="serviceAccount:drt-runner@my-project.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### HashiCorp Vault — `vault://`

```
vault://<mount>/data/<path>#<key>
```

- The path mirrors Vault's own raw KV v2 HTTP path — mount point, the
  literal `data` segment KV v2's API inserts to distinguish a data read
  from a metadata/delete operation, then the logical secret path. It's
  the same shape `vault kv get` and curl examples against Vault already
  use.
- **`#<key>` is required.** A KV v2 secret's payload is always a field
  map — there's no "the secret is just one string" case to fall back to
  the way AWS/GCP have, so a field must always be named.
- Requires: `pip install drt-core[vault]`
- Auth: `VAULT_ADDR` / `VAULT_TOKEN` env vars (`hvac.Client()`'s own
  defaults) — nothing drt-specific to configure. Unlike the AWS/GCP SDKs,
  hvac does not refresh a token on its own — see the caching note below.

Vault policy granting read access to a path prefix:

```hcl
path "secret/data/drt/*" {
  capabilities = ["read"]
}
```

## Caching, and what it means for `drt serve`

A resolved value is cached, keyed by the full URI — a run commonly
resolves the same credential more than once (a validation pass, a
connection test, the real connection), and unlike an env var or
`secrets.toml` read, a provider fetch is a network call worth not
repeating. Each entry expires after a **TTL**, default **300 seconds**
(configurable via `DRT_SECRET_CACHE_TTL_SECONDS`, read on each lookup —
changing it takes effect without restarting the process). A lookup past
the TTL transparently refetches and replaces the cached value.

For a `drt run` / `drt test` / `drt build` invocation — which exits in
seconds to minutes — this is invisible. For
[`drt serve`](using-webhook-trigger.md), a long-lived process that
re-enters credential resolution on every triggered sync, the TTL is what
keeps a rotated secret from being held indefinitely: it's picked up
within one TTL window, no manual restart needed. Setting
`DRT_SECRET_CACHE_TTL_SECONDS` to a non-positive value disables caching
entirely — every lookup refetches — useful if your rotation window is
tighter than the default and you'd rather pay the network call than wait
out the TTL.

The Vault leg goes one step further for a related reason: `hvac.Client()`
captures `VAULT_TOKEN` once at construction and never refreshes it, and
Vault tokens are conventionally short-TTL — so unlike the AWS/GCP legs,
the Vault *client* is rebuilt on every fetch rather than cached, avoiding
a worse failure mode (every fetch failing outright once the token
expires, rather than merely serving a value that's stale for at most the
value-cache's TTL above). This is already handled; nothing to configure.

## What this isn't

- **Not secret writing or rotation management.** drt only reads. Rotate
  secrets through the provider's own tooling; drt picks up the new value
  on its next fetch (subject to the caching note above).
- **Not encryption of local files.** Provider URIs and local file encryption
  are separate mechanisms. See [Encrypt Local Secrets at Rest with
  age](secrets-encryption.md) when you deliberately use `.drt/secrets.toml`
  instead of an external store.
- **Not OAuth token brokering.** A provider URI resolves to a static
  secret value (a password, an API key) each time it's fetched — it does
  not manage an OAuth token lifecycle.
- **Not a third-party plugin surface — yet.** The provider registry is
  structured so a future plugin mechanism can add providers without
  changing how `resolve_env` dispatches, but there is no mechanism for
  installing a third-party provider today.

## Related

- `docs/llm/API_REFERENCE.md` — the full resolution chain, including
  `.drt/secrets.toml`
