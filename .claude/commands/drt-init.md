
Guide the user through initializing a new drt project.

## Steps

1. Confirm drt is installed. Pick the install line for the user's source:
   ```bash
   pip install drt-core                 # core (DuckDB / SQLite / REST API source — no extras)
   pip install drt-core[bigquery]       # BigQuery source
   pip install drt-core[postgres]       # Postgres / Redshift source
   pip install drt-core[mysql]          # MySQL source
   pip install drt-core[clickhouse]     # ClickHouse source
   pip install drt-core[snowflake]      # Snowflake source
   pip install drt-core[databricks]     # Databricks source
   pip install drt-core[sqlserver]      # SQL Server source
   pip install drt-core[deltalake]     # Delta Lake source
   pip install drt-core[iceberg]       # Iceberg source
   ```
   (Or `uv add drt-core[...]` if the user prefers uv.)

2. Pick the right starter shape. Two flows produce the same project skeleton:

   **A) Template scaffolds (fastest, ~3 commands)** — best when the user just wants something running:
   ```bash
   mkdir my-drt-project && cd my-drt-project
   drt init --template duckdb_to_rest        # DuckDB → REST API (no accounts needed, uses httpbin.org)
   ```
   Other templates:
   ```bash
   drt init --template list                  # see all available templates
   drt init --template postgres_to_slack     # operational alerts
   drt init --template duckdb_to_hubspot     # CRM-style upsert
   ```
   Each template prints next-steps for the env vars / source data it needs.

   **B) Interactive wizard (guided)** — best when the user wants to be walked through a real warehouse setup:
   ```bash
   mkdir my-drt-project && cd my-drt-project
   drt init
   ```
   Prompts for project name, source type (any of: **bigquery / duckdb / sqlite / postgres / redshift / clickhouse / snowflake / mysql / databricks / sqlserver / deltalake / iceberg / rest_api**), source-specific connection fields, and auth method.

   Either flow writes:
   ```
   my-drt-project/
   ├── drt_project.yml
   └── syncs/<name>.yml      # first sync, runnable
   ```
   plus credentials to `~/.drt/profiles.yml`.

3. Set up auth for the chosen source (only the relevant bullet):
   - **BigQuery**: `gcloud auth application-default login` or `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json`
   - **Postgres / Redshift / MySQL / ClickHouse / Snowflake / SQL Server**: set the password env var the profile references (e.g. `export DRT_PG_PASSWORD=...`)
   - **Databricks**: set `DATABRICKS_TOKEN` (personal access token from Workspace → User Settings)
   - **DuckDB / SQLite**: nothing — file path only
   - **REST API source**: set the bearer token env var the profile references (e.g. `export REST_API_TOKEN=...`)

4. Validate the setup:
   ```bash
   drt doctor       # env-level triage — catches missing env vars, malformed profile, etc.
   drt validate     # YAML schema check
   drt list         # confirm syncs are discovered
   ```

5. Offer to create a first sync using the `/drt-create-sync` skill.

## Tips

- `drt_project.yml` selects which profile from `~/.drt/profiles.yml` to use; override per-run with `--profile <name>` or `DRT_PROFILE`.
- Put each sync in a separate `syncs/<name>.yml` file (sync discovery is glob-based).
- `drt sources --detailed` and `drt destinations --detailed` print every connector's required env vars and a sample YAML stanza — useful when hand-authoring beyond the templates.
- `drt run --dry-run` runs through the engine without writing data; `--dry-run --diff` previews record-level changes for queryable destinations.
- `drt status` shows recent run results; `drt status --output json` is the CI-friendly form.
- For non-US BigQuery datasets, set `location` in `profiles.yml` (e.g. `"EU"`, `"asia-northeast1"`).
- Want to try drt without installing locally? Open the repo in GitHub Codespaces — the devcontainer ships drt + a seeded DuckDB warehouse + the `duckdb_to_rest` template pre-scaffolded.
