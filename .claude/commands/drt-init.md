Guide the user through initializing a new drt project.

## Steps

1. Confirm the user has drt installed:
   ```bash
   pip install drt-core[bigquery]   # BigQuery
   # or
   uv add drt-core[bigquery]
   ```

2. Create and enter a project directory:
   ```bash
   mkdir my-drt-project && cd my-drt-project
   drt init
   ```
   `drt init` will prompt for:
   - Project name
   - Source type (bigquery / duckdb / postgres)
   - GCP project + dataset (if BigQuery)
   - Auth method (Application Default Credentials or keyfile)

3. This creates:
   ```
   my-drt-project/
   ├── drt_project.yml
   └── syncs/
       └── example_sync.yml
   ```
   And writes credentials to `~/.drt/profiles.yml`.

4. For BigQuery, ensure credentials are set up:
   ```bash
   gcloud auth application-default login
   # or set GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
   ```

5. Validate the setup:
   ```bash
   drt validate
   drt list
   ```

6. Offer to create a first sync using `/drt-create-sync`.

## Tips

- `drt_project.yml` selects which profile from `~/.drt/profiles.yml` to use
- Put each sync in a separate `syncs/<name>.yml` file
- Use `drt run --dry-run` to test without writing data
- Use `drt status` to check recent run results
