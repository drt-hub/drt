# dagster-drt

Dagster integration for [drt](https://github.com/drt-hub/drt) — expose drt syncs as Dagster assets.

## Install

```bash
pip install dagster-drt
```

## Usage

```python
from dagster import Definitions
from dagster_drt import drt_assets

defs = Definitions(assets=drt_assets(project_dir="path/to/drt-project"))
```

Then run:
```bash
dagster dev
```
