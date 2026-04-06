<!-- i18n-sync: base=README.md, hash=da09e7fded81c276bc17449c8eae4feea3a4afe4 -->

[English](./README.md) | [日本語](./README.ja.md)

> **Note:** この翻訳は最新でない可能性があります。正確な情報は [README.md](README.md) を参照してください。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/drt-hub/.github/main/profile/assets/logo-dark.svg">
  <img src="https://raw.githubusercontent.com/drt-hub/.github/main/profile/assets/logo.svg" alt="drt logo" width="200">
</picture>

# drt — data reverse tool

> コードファーストのデータスタック向けのリバースETLツール。

[![CI](https://github.com/drt-hub/drt/actions/workflows/ci.yml/badge.svg)](https://github.com/drt-hub/drt/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/drt-hub/drt/graph/badge.svg)](https://codecov.io/gh/drt-hub/drt)
[![PyPI](https://img.shields.io/pypi/v/drt-core)](https://pypi.org/project/drt-core/)
[![drt-core downloads](https://img.shields.io/pepy/dt/drt-core?label=drt-core%20downloads)](https://pepy.tech/projects/drt-core)
[![dagster-drt downloads](https://img.shields.io/pepy/dt/dagster-drt?label=dagster-drt%20downloads)](https://pepy.tech/projects/dagster-drt)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/drt-core)](https://pypi.org/project/drt-core/)

**drt** は、YAMLとCLIを使って、データウェアハウスから外部サービスへデータを同期します（宣言的に設定可能）。
`dbt run` → `drt run`のイメージです。同じ開発体験で、データの流れが逆になります。

<p align="center">
  <img src="docs/assets/quickstart.gif" alt="drt quickstart demo" width="700">
</p>

```bash
pip install drt-core          # core (DuckDB included)
drt init && drt run
```

---

## なぜdrtなのか？

| 問題 | drtの回答 |
|---------|-------------|
| Census / Hightouch は高価なSaaS | 無料のセルフホスト型OSS |
| GUI優先のツールはCI/CDには適さない | CLI + YAML、Gitネイティブ |
| dbt/dltエコシステムには逆方向の仕組みがない | 同じ哲学、同じ開発体験（DX） |
| LLM/MCP時代にはGUI SaaSは過剰 | LLM前提で設計 |

---

## クイックスタート

クラウドアカウントは不要です。DuckDBを使って、ローカル環境で約5分で実行できます。

### 1. インストール

```bash
pip install drt-core
```

> クラウドソースの場合：`pip install drt-core[bigquery]`、`drt-core[postgres]` など。

### 2. プロジェクトのセットアップ

```bash
mkdir my-drt-project && cd my-drt-project
drt init   # select "duckdb" as source
```

### 3. サンプルデータの作成

```bash
python -c "
import duckdb
c = duckdb.connect('warehouse.duckdb')
c.execute('''CREATE TABLE IF NOT EXISTS users AS SELECT * FROM (VALUES
  (1, 'Alice', 'alice@example.com'),
  (2, 'Bob',   'bob@example.com'),
  (3, 'Carol', 'carol@example.com')
) t(id, name, email)''')
c.close()
"
```

### 4. 同期の作成

```yaml
# syncs/post_users.yml
name: post_users
description: "POST user records to an API"
model: ref('users')
destination:
  type: rest_api
  url: "https://httpbin.org/post"
  method: POST
  headers:
    Content-Type: "application/json"
  body_template: |
    { "id": {{ row.id }}, "name": "{{ row.name }}", "email": "{{ row.email }}" }
sync:
  mode: full
  batch_size: 1
  on_error: fail
```

### 5. 実行

```bash
drt run --dry-run   # preview, no data sent
drt run             # run for real
drt status          # check results
```

> 詳細は [examples/](examples/) を参照してください（Slack、Google Sheets、HubSpot、GitHub Actions など）。

---

## CLIリファレンス

```bash
drt init                    # initialize project
drt list                    # list sync definitions
drt run                     # run all syncs
drt run --select <name>     # run a specific sync
drt run --dry-run           # dry run
drt run --verbose           # show row-level error details
drt validate                # validate sync YAML configs
drt status                  # show recent sync status
drt status --verbose        # show per-row error details
drt mcp run                 # start MCP server (requires drt-core[mcp])
```

---

## MCPサーバー

drtをClaude、Cursor、またはMCP互換クライアントに接続することで、AI環境から離れることなく、同期の実行、ステータスの確認、設定の検証が可能になります。

```bash
pip install drt-core[mcp]
drt mcp run
```

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "drt": {
      "command": "drt",
      "args": ["mcp", "run"]
    }
  }
}
```

**利用可能なMCPツール：**

| ツール | 機能 |
|------|-------------|
| `drt_list_syncs` | 同期定義の一覧を表示 |
| `drt_run_sync` | 同期を実行（`dry_run`対応） |
| `drt_get_status` | 前回の実行結果を取得 |
| `drt_validate` | 同期YAML設定を検証 |
| `drt_get_schema` | 設定ファイルのJSONスキーマを返す |

---

## Claude CodeのためのAIスキル

Claude Codeの公式スキルをインストールすると、チャットインターフェースからYAMLの生成、エラーのデバッグ、他のツールからの移行が可能になります。

### プラグインマーケットプレイスからインストール（推奨）

```bash
/plugin marketplace add drt-hub/drt
/plugin install drt@drt-hub
```

> **ヒント:** drtが更新された際に常に最新のスキルを利用できるよう、自動更新を有効にしてください：
> `/plugin` → Marketplaces → drt-hub → Enable auto-update

### 手動インストール（スラッシュコマンド）

`.claude/commands/`のファイルをdrtプロジェクトの`.claude/commands/`ディレクトリにコピーしてください。

| スキル | トリガー | 説明 |
|-------|---------|-------------|
| `/drt-create-sync` | "create a sync" | インテントから有効な同期YAMLを生成 |
| `/drt-debug` | "sync failed" | エラーを診断し、修正方法を提案 |
| `/drt-init` | "set up drt" | プロジェクト初期化を案内 |
| `/drt-migrate` | "migrate from Census" | 既存の設定をdrt YAMLに変換 |

---

## コネクタ

| 種類 | 名前 | ステータス | インストール |
|------|------|--------|---------|
| **Source** | BigQuery | ✅ v0.1 | `pip install drt-core[bigquery]` |
| **Source** | DuckDB | ✅ v0.1 | (core) |
| **Source** | PostgreSQL | ✅ v0.1 | `pip install drt-core[postgres]` |
| **Source** | Snowflake | 🗓 planned | `pip install drt-core[snowflake]` |
| **Source** | SQLite | ✅ v0.4.2 | (core) |
| **Source** | Redshift | ✅ v0.3.4 | `pip install drt-core[redshift]` |
| **Source** | ClickHouse | ✅ v0.4.3 | `pip install drt-core[clickhouse]` |
| **Source** | MySQL | 🗓 planned | `pip install drt-core[mysql]` |
| **Destination** | REST API | ✅ v0.1 | (core) |
| **Destination** | Slack Incoming Webhook | ✅ v0.1 | (core) |
| **Destination** | Discord Webhook | ✅ v0.4.2 | (core) |
| **Destination** | GitHub Actions (workflow_dispatch) | ✅ v0.1 | (core) |
| **Destination** | HubSpot (Contacts / Deals / Companies) | ✅ v0.1 | (core) |
| **Destination** | Google Sheets | ✅ v0.4 | `pip install drt-core[sheets]` |
| **Destination** | PostgreSQL (upsert) | ✅ v0.4 | `pip install drt-core[postgres]` |
| **Destination** | MySQL (upsert) | ✅ v0.4 | `pip install drt-core[mysql]` |
| **Destination** | CSV / JSON file | 🗓 v0.5 | (core) |
| **Destination** | Salesforce | 🗓 v0.6 | `pip install drt-core[salesforce]` |
| **Destination** | Notion | 🗓 planned | (core) |
| **Destination** | Linear | 🗓 planned | (core) |
| **Destination** | SendGrid | 🗓 planned | (core) |
| **Integration** | Dagster | ✅ v0.4 | `pip install dagster-drt` |
| **Integration** | Airflow | 🗓 v0.6 | `pip install airflow-drt` |
| **Integration** | dbt manifest reader | ✅ v0.4 | (core) |

---

## ロードマップ

> **詳細な計画と進捗 → [GitHub Milestones](https://github.com/drt-hub/drt/milestones)**
> **貢献したい方はこちら → [Good First Issues](https://github.com/drt-hub/drt/issues?q=is%3Aopen+label%3A%22good+first+issue%22)**

| バージョン | 内容 |
|---------|-------|
| **v0.1** ✅ | BigQuery / DuckDB / Postgres sources · REST API / Slack / GitHub Actions / HubSpot destinations · CLI · dry-run |
| **v0.2** ✅ | Incremental sync (`cursor_field` watermark) · retry config per-sync |
| **v0.3** ✅ | MCP Server (`drt mcp run`) · AI Skills for Claude Code · LLM-readable docs · row-level errors · security hardening · Redshift source |
| **v0.4** ✅ | Google Sheets / PostgreSQL / MySQL destinations · dagster-drt · dbt manifest reader · type safety overhaul |
| [v0.5](https://github.com/drt-hub/drt/milestone/2) | Snowflake source · CSV/JSON + Parquet destinations · test coverage · Docker |
| [v0.6](https://github.com/drt-hub/drt/milestone/3) | Salesforce · Airflow integration · Jira / Twilio / Intercom destinations |
| [v0.7](https://github.com/drt-hub/drt/milestone/4) | DWH destinations (Snowflake / BigQuery / ClickHouse / Databricks) · Cloud storage (S3 / GCS / Azure Blob) |
| [v0.8](https://github.com/drt-hub/drt/milestone/5) | Lakehouse sources (Delta Lake / Apache Iceberg) |
| v1.x | Rust engine (PyO3) |

---

## オーケストレーション: dagster-drt

コミュニティによって維持管理されている [Dagster](https://dagster.io/) との統合。drt の同期を、可観測性を備えた Dagster アセットとして公開します。

```bash
pip install dagster-drt
```

```python
from dagster import AssetExecutionContext, Definitions
from dagster_drt import drt_assets, DagsterDrtResource

@drt_assets(project_dir="path/to/drt-project")
def my_syncs(context: AssetExecutionContext, drt: DagsterDrtResource):
    yield from drt.run(context=context)

defs = Definitions(
    assets=[my_syncs],
    resources={"drt": DagsterDrtResource(project_dir="path/to/drt-project")},
)
```

詳細な API ドキュメント（Translator、Pipes サポート、DrtConfig のドライラン、MaterializeResult）については、[dagster-drt README](integrations/dagster-drt/README.md) を参照してください。

---

## エコシステム

drtは最新のデータスタックと競合するのではなく、共存するように設計されています:

<p align="center">
  <img src="docs/assets/ecosystem.png" alt="drt ecosystem — dlt load, dbt transform, drt activate" width="700">
</p>

---

## コントリビュート

[CONTRIBUTING.md](CONTRIBUTING.md)を参照してください。

## 免責事項

drtは独立したオープンソースプロジェクトであり、dbt Labs、dlt-hub、またはその他のいかなる企業とも提携、承認、または後援関係にありません。

「dbt」はdbt Labs, Inc.の登録商標です。
「dlt」はdlt-hubによってメンテナンスされているプロジェクトです。

drtは現代のデータスタックの一部としてこれらのツールを補完するように設計されていますが、独自のコードベースとメンテナーを持つ独立したプロジェクトです。

## ライセンス

Apache 2.0 — [LICENSE](LICENSE)を参照してください。
