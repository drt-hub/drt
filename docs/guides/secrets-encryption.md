# Encrypt Local Secrets at Rest with age

`.drt/secrets.toml` is convenient when a local machine or small team does not
use an external secret manager, but the file is plaintext while it is on disk.
drt can encrypt that file with an age X25519 identity and transparently decrypt
it in memory when a command resolves a credential.

This feature is deliberately separate from [secret provider
URIs](secret-provider-uris.md). Provider URIs fetch credentials from AWS/GCP
Secret Manager or Vault at runtime; local encryption keeps the existing
`secrets.toml` workflow and protects that one file at rest.

## Quick Start

Install the optional encryption backend:

```bash
pip install 'drt-core[encryption]'
```

Generate an age X25519 identity without installing a separate `age` binary:

```bash
export DRT_SECRETS_KEY="$(python -c 'from pyrage import x25519; print(x25519.Identity.generate())')"
```

`DRT_SECRETS_KEY` contains an `AGE-SECRET-KEY-...` identity. Store it in a
password manager or your CI system's encrypted secret store before continuing;
if it is lost, the encrypted file cannot be recovered. Do not commit the key or
put its literal value in shell history, project YAML, or `.env` files.

Encrypt the project-local secrets file:

```bash
drt encrypt .drt/secrets.toml
```

This creates `.drt/secrets.toml.enc`. The plaintext file is intentionally not
deleted. Run a normal command to verify decryption, then remove it yourself:

```bash
drt run --select a_safe_sync
rm .drt/secrets.toml
```

The generated `.enc` file may be copied to CI or shared storage. It is safe to
commit only to the extent that the age identity remains secret: anyone with
`DRT_SECRETS_KEY` can decrypt every file encrypted to that identity.

## Automatic in-memory decryption

Every credential path that currently consults `.drt/secrets.toml` also checks
`.drt/secrets.toml.enc`. When the encrypted file exists, it takes precedence
over plaintext, is decrypted in memory, and is parsed as TOML without writing a
temporary plaintext file. Commands such as `drt run`, `drt build`, `drt test`,
and `drt serve` inherit this behavior through the same `resolve_env()`
credential path.

If the encrypted file exists but `DRT_SECRETS_KEY` is absent, invalid, or does
not match, drt stops with an actionable error. It never silently falls back to
the adjacent plaintext file; doing so would make a stale plaintext copy mask a
broken encryption setup.

Credential resolution order remains:

```
explicit YAML value > OS environment variable > encrypted/plain secrets.toml > provider URI
```

## Decrypting to disk

Manual decryption is only needed when you want to edit or inspect the TOML:

```bash
drt decrypt .drt/secrets.toml.enc
```

This writes `.drt/secrets.toml` with owner-only permissions on POSIX. Both
commands refuse to overwrite their output; pass `--force` when replacement is
intentional. Remove the plaintext again after editing and re-encrypting it.

## Key management

- Keep one backed-up copy of the identity in a password manager or secrets
  vault. The `.enc` file does not contain a recovery key.
- In CI, store the identity as the protected secret `DRT_SECRETS_KEY` and expose
  it only to jobs that run drt.
- Treat identity rotation as a decrypt-and-re-encrypt operation: decrypt with
  the old identity, set a newly generated identity, then run
  `drt encrypt --force .drt/secrets.toml`.
- Use separate identities when projects should not share a decryption boundary.

Only `.drt/secrets.toml` is covered. drt state, history, and DLQ files do not
contain connector credentials and are not encrypted by these commands.
