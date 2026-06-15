<!-- i18n-sync: base=README.md, hash=a05482930e95f0db036d95c9244a492650d91d18 -->

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
[![GitHub Sponsors](https://img.shields.io/static/v1?label=Sponsor&message=%E2%9D%A4&logo=GitHub&color=%23fe8e86)](https://github.com/sponsors/masukai)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/drt-hub/drt)

<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-15-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

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

クラウドアカウントは不要 — DuckDB と httpbin.org でコマンド3つから。

```bash
pip install drt-core
mkdir my-drt-project && cd my-drt-project
drt init --template duckdb_to_rest
```

実行可能な `syncs/duckdb_to_rest.yml` がスキャフォールドされます。DuckDB
に小さなテーブルをシードして実行:

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
drt run --dry-run   # プレビュー、データは送信されない
drt run             # httpbin.org に各行を POST
drt status          # 結果を確認
```

### その他のスタータテンプレート

```bash
drt init --template list             # 利用可能なテンプレート一覧
drt init --template postgres_to_slack
drt init --template duckdb_to_hubspot
```

各テンプレートは必要な環境変数 / ソースデータの次のステップを表示します。
完全なコレクションは [examples/](examples/) (Discord、Google Sheets、
GitHub Actions、MySQL、ClickHouse、BigQuery …)、コネクタ別リファレンスは
[docs/connectors/](docs/connectors/) を参照。

### 同期のカスタマイズ

プロファイル + プロジェクトのセットアップを案内するウィザードが必要な場合:

```bash
drt init   # 対話形式 — ソース選択、プロファイル設定、プロジェクトスキャフォールド
```

どちらのフローも同じプロジェクト構造 (`drt_project.yml`、`syncs/`、
`.drt/`) を生成します。`drt sources --detailed` と
`drt destinations --detailed` は各コネクタの必要な環境変数とサンプル
YAML スタンザを表示します — テンプレートを超えて手で書くときに便利です。

---

## CLIリファレンス

```bash
drt init                    # プロジェクトの初期化
drt list                    # 同期定義の一覧
drt run                     # 全同期を実行
drt run --select <name>     # 特定の同期を実行
drt run --dry-run           # ドライラン
drt run --verbose           # 行レベルのエラー詳細を表示
drt run --output json       # CI/スクリプト向け構造化JSON出力
drt run --profile prd       # プロファイル切り替え（DRT_PROFILE環境変数でも可）
drt sources                 # 利用可能なソースコネクタを一覧
drt destinations            # 利用可能なデスティネーションコネクタを一覧
drt run --all               # 全同期を検出して実行
drt run --select tag:<tag>  # タグに一致する同期を実行
drt run --threads 4         # 並列同期実行
drt run --log-format json   # 構造化JSONログをstderrに出力
drt run --cursor-value '…'  # バックフィル用にウォーターマークカーソルを上書き
drt test                    # 同期後の検証テストを実行
drt test --select <name>    # 特定の同期テストを実行
drt validate                # 同期YAML設定を検証
drt status                  # 直近の同期ステータスを表示
drt status --output json    # JSON形式でステータスを出力
drt serve                   # HTTPウェブフックエンドポイントを起動
drt mcp run                 # MCPサーバーを起動（drt-core[mcp]が必要）
drt --install-completion    # シェル補完をインストール（bash/zsh/fish）
drt --show-completion       # 補完スクリプトを表示
```

### シェル補完

bash、zsh、fishのシェル補完に対応しています：

```bash
# 推奨：現在のシェルに自動インストール（冪等）
drt --install-completion

# 手動でシェル設定に追加（対象シェルから一度だけ実行）
drt --show-completion >> ~/.bashrc   # bash
drt --show-completion >> ~/.zshrc    # zsh
drt --show-completion > ~/.config/fish/completions/drt.fish  # fish
```

> **注意:** `--show-completion` は*現在のシェル*用のスクリプトを出力します。設定したいシェルから実行してください。手動の `>>` 追記は冪等ではありません — 一度だけ実行してください。

インストール後、シェルを再起動するとコマンドやオプションのタブ補完が利用可能になります。

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
| `drt_run_sync` | 同期を実行（`dry_run` + `compute_diff` で `--diff` 同等） |
| `drt_run_test` | 同期後の検証テストを実行（`drt test` 相当） |
| `drt_get_status` | 前回の実行結果を取得 |
| `drt_get_history` | 直近の同期実行履歴を取得 |
| `drt_validate` | 同期YAML設定を検証 |
| `drt_get_schema` | 設定ファイルのJSONスキーマを返す |
| `drt_list_connectors` | 利用可能なソースとデスティネーションを一覧 |
| `drt_doctor` | 環境診断（`drt doctor` 相当） |

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
| `/drt-debug` | "sync failed" | 特定のエラーを診断し、修正方法を提案 |
| `/drt-troubleshoot` | "drt isn't working" | 上から下まで全体を診断するチェックリストを案内 |
| `/drt-init` | "set up drt" | プロジェクト初期化を案内 |
| `/drt-migrate` | "migrate from Census" | 既存の設定をdrt YAMLに変換 |

---

## コネクタ

> コネクタ別リファレンス: [docs/connectors/](docs/connectors/) · CLI からも辿れます: `drt sources --detailed` / `drt destinations --detailed`

### ソース

| コネクタ | ステータス | インストール | 認証 |
|-----------|--------|---------|------|
| BigQuery | ✅ v0.1 | `pip install drt-core[bigquery]` | Application Default / Service Account Keyfile |
| DuckDB | ✅ v0.1 | (core) | ファイルパス |
| PostgreSQL | ✅ v0.1 | `pip install drt-core[postgres]` | パスワード（環境変数） |
| Snowflake | ✅ v0.5 | `pip install drt-core[snowflake]` | パスワード（環境変数） |
| SQLite | ✅ v0.4.2 | (core) | ファイルパス |
| Redshift | ✅ v0.3.4 | `pip install drt-core[redshift]` | パスワード（環境変数） |
| ClickHouse | ✅ v0.4.3 | `pip install drt-core[clickhouse]` | パスワード（環境変数） |
| MySQL | ✅ v0.5 | `pip install drt-core[mysql]` | パスワード（環境変数） |
| Databricks | ✅ v0.6 | `pip install drt-core[databricks]` | Access Token（環境変数） |
| SQL Server | ✅ v0.6 | `pip install drt-core[sqlserver]` | パスワード（環境変数） |
| REST API | ✅ v0.7 | (core) | Bearer / API Key / Basic / OAuth2 |

### デスティネーション

| コネクタ | ステータス | インストール | 認証 |
|-----------|--------|---------|------|
| REST API | ✅ v0.1 | (core) | Bearer / API Key / Basic / OAuth2 |
| Slack Incoming Webhook | ✅ v0.1 | (core) | Webhook URL |
| Discord Webhook | ✅ v0.4.2 | (core) | Webhook URL |
| GitHub Actions | ✅ v0.1 | (core) | Token（環境変数） |
| HubSpot | ✅ v0.1 | (core) | Token（環境変数） |
| Zendesk | ✅ v0.7 | (core) | Basic（メールアドレス + API トークン） |
| Amplitude | ✅ v0.7 | (core) | プロジェクト API キー（環境変数） |
| Google Ads | ✅ v0.6 | (core) | OAuth2 Client Credentials |
| Google Sheets | ✅ v0.4 | `pip install drt-core[sheets]` | Service Account Keyfile |
| PostgreSQL (upsert) | ✅ v0.4 | `pip install drt-core[postgres]` | パスワード（環境変数） |
| MySQL (upsert) | ✅ v0.4 | `pip install drt-core[mysql]` | パスワード（環境変数） |
| ClickHouse | ✅ v0.5 | `pip install drt-core[clickhouse]` | パスワード（環境変数） |
| Parquet file | ✅ v0.5 | `pip install drt-core[parquet]` | ファイルパス |
| Amazon S3 | ✅ v0.7.9 | `pip install drt-core[s3]` | AWS 認証チェイン / 環境変数 |
| Google Cloud Storage | ✅ v0.7.9 | `pip install drt-core[gcs]` | Application Default / サービスアカウント JSON |
| Azure Blob Storage | ✅ v0.7.9 | `pip install drt-core[azure]` | 接続文字列環境変数 / DefaultAzureCredential |
| Microsoft Teams Webhook | ✅ v0.5 | (core) | Webhook URL |
| CSV / JSON / JSONL file | ✅ v0.5 | (core) | ファイルパス |
| Jira | ✅ v0.5 | (core) | Basic（メール + APIトークン） |
| Linear | ✅ v0.5 | (core) | API Key（環境変数） |
| SendGrid | ✅ v0.5 | (core) | API Key（環境変数） |
| Notion | ✅ v0.6 | (core) | Bearer Token（環境変数） |
| Twilio SMS | ✅ v0.6 | (core) | Basic（Account SID + Auth Token） |
| Intercom | ✅ v0.6 | (core) | Bearer Token（環境変数） |
| Email SMTP | ✅ v0.6 | (core) | ユーザー名/パスワード（環境変数） |
| Salesforce Bulk API 2.0 | ✅ v0.6 | (core) | OAuth2（username-password） |
| Staged Upload | ✅ v0.6 | (core) | プロバイダーごとに設定 |
| Elasticsearch / OpenSearch | ✅ v0.7.9 | (core) | API キー / HTTP Basic（環境変数） |
| Snowflake | ✅ v0.7 | `pip install drt-core[snowflake]` | パスワード（環境変数） |
| Databricks Delta Lake | ✅ v0.7.9 | `pip install drt-core[databricks]` | Personal Access Token（環境変数） |

### インテグレーション

| コネクタ | ステータス | インストール |
|-----------|--------|---------|
| Dagster | ✅ v0.4 | `pip install dagster-drt` |
| Prefect | ✅ v0.6 | (core) |
| Airflow | ✅ v0.6 | (core) |
| dbt manifest reader | ✅ v0.4 | (core) |

---

## ロードマップ

> **今後のリリース → [ROADMAP.md](ROADMAP.md)** （スコープ・テーマ・目標時期）
> **Issue 単位の追跡 → [GitHub Milestones](https://github.com/drt-hub/drt/milestones)**
> **貢献したい方はこちら → [Good First Issues](https://github.com/drt-hub/drt/issues?q=is%3Aopen+label%3A%22good+first+issue%22)**

**リリース済み:**

| バージョン | 内容 |
|---------|-------|
| **v0.1** ✅ | BigQuery / DuckDB / Postgres sources · REST API / Slack / GitHub Actions / HubSpot destinations · CLI · dry-run |
| **v0.2** ✅ | Incremental sync (`cursor_field` watermark) · retry config per-sync |
| **v0.3** ✅ | MCP Server (`drt mcp run`) · AI Skills for Claude Code · LLM-readable docs · row-level errors · security hardening · Redshift source |
| **v0.4** ✅ | Google Sheets / PostgreSQL / MySQL destinations · dagster-drt · dbt manifest reader · type safety overhaul |
| **v0.5** ✅ | Snowflake / MySQL sources · ClickHouse / Parquet / Teams / CSV+JSON / Jira / Linear / SendGrid destinations · `drt test` · `--output json` · `--profile` · `${VAR}` 環境変数展開 · dbt manifest · secrets.toml · Docker |
| **v0.5.4** ✅ | `destination_lookup` — 同期中にデスティネーションDBからFK値を解決（MySQL / Postgres / ClickHouse） |
| **v0.6** ✅ | Databricks / SQL Server sources · Notion / Twilio / Intercom / Email SMTP / Salesforce Bulk / Staged Upload destinations · Airflow / Prefect integrations · `drt serve` · `drt sources` / `drt destinations` · `--threads` 並列実行 · `--log-format json` · `--cursor-value` · `watermark.default_value` · テストバリデータ（freshness, unique, accepted_values） · JSON Schema validation · GOVERNANCE.md |
| **v0.7** ✅ | **Production Ready** — SIGTERM/SIGINT グレースフルシャットダウン · per-destination retry override · sync 実行履歴 · zero-downtime atomic table swap · `json_columns` 設定 · FK存在チェック (`lookups.check_only`) · Slack/webhook 失敗通知 · `drt doctor` · `--quiet` フラグ · `drt test --output json` / `--dry-run` · Snowflake destination · GitHub Codespaces プレイグラウンド · `OPEN_CORE.md` |
| **v0.7.1** ✅ | `drt run --dry-run --diff` でレコード単位プレビュー · tz-aware カーソル文字列化バグ修正 · `on_error=fail` を Notion / REST API / Email SMTP に整合 · `VERSIONING.md` セマンティクスバージョニング & 非推奨ポリシー |
| **v0.7.2** ✅ | **opt-in 匿名テレメトリ**（PostHog Cloud EU、デフォルト off、許可リスト式 payload、`DO_NOT_TRACK` 尊重）· `drt validate` の非推奨警告 · Postgres destination の `psycopg2.sql` 安全な SQL 合成 |
| **v0.7.3** ✅ | **パッチ** — Postgres の `schema.table` 識別子合成の修正 (#442, PR #498): `marketing.events` のような schema 修飾名が単一 identifier として double-quote されていたバグを修正し、schema と relation を分離して合成 |
| **v0.7.4** ✅ | **パッチ** — MySQL の `schema.table` 識別子クオートの修正 (#511, PR #514): `mydb.scores` が replace / insert / upsert / 行カウントの各 SQL パスで `` `mydb`.`scores` `` に正しくクオートされるよう `_quote_ident` を統一適用。PR #514 は v0.7.3 タグの2日後に `main` にマージされたため `drt-core==0.7.3` の wheel には含まれていない — `#511` を踏んでいるユーザは `drt-core>=0.7.4` へアップグレードを |
| **v0.7.5** ✅ | **Production Ready follow-up #3 + Tech Foundation Hardening** — [Tech Foundation Hardening epic (#538)](https://github.com/drt-hub/drt/issues/538) の 11 子 issue をクローズ: CI nightly + publish gate + CodeQL + pip-audit + SBOM · DuckDB E2E ハーネス + 境界テスト · `ErrorFormatter` + `drt sources/destinations --detailed` + `drt init --template` の UX · `SyncObserver` で engine I/O 境界整理 · destinations serializer + config base class 統合 · `cli/main.py` 分割 Phase 1。あわせて REST API source polish · sync catalog (#499 P1+P2) · `drt_run_test` MCP ツール · OpenTelemetry Phase 1 config · ハードコードシークレット検知 · orphan shadow テーブルクリーンアップ も同梱。新規コネクタなし、破壊変更なし。 |
| **v0.7.6** ✅ | **小規模フォローアップ** — **Amplitude destination** (#574, Identify API + HTTP V2 events API) · **`tojson_safe` Jinja2 フィルタ** (#580, PR #581) で REST API `body_template` の `datetime` / `Decimal` / `UUID` カラムを model SQL 側で `CAST(... AS STRING)` せずに送れるように · `drt run --log-format` を typer 0.26.1 互換に修正 (#577, PR #578) · `ErrorFormatter` の stage 推定を engine-emitted attr に retrofit (PR #571, #544 のトレースバック解析を置き換え) · `cli/main.py` 分割 Phase 2a (PR #572)。破壊変更なし — v0.7.5 からドロップイン更新可。 |
| **v0.7.7** ✅ | **`sync.mode: mirror` を SQL destinations 全体に展開** — ソース行を upsert したうえで、ソースで観測されなかった `upsert_key` を持つ destination 行を DELETE する新しい差分同期モード。`replace` モードの TRUNCATE / 全 INSERT のオーバーヘッドなしで実現。**Postgres (#596) · MySQL (#597) · ClickHouse (#598) · Snowflake (#599)** の4 destinations 同時対応 — `#340` は SQL set としてクローズ。あわせて **`cli/main.py` 分割完了** (Phase 2b PR (a) + PR (b) + tighten で v0.7.5 baseline からの 1706 → 164 LOC, -90%)、`FakeSource` + destination contract test framework (#592–#595)、CI `check-changelog-required` の warn-only ガード (#590)、GCS storage import の mypy fix (#588, @cian-ps の報告)、CI install line 拡張による silently-skipped SQL destination test ~102 件の解放 (total coverage 82.68 → 85.29) を同梱。破壊変更なし — v0.7.6 からドロップイン更新可。 |
| **v0.7.8** ✅ | **コミュニティ駆動フォローアップ: Mixpanel destination + ClickHouse identifier 修正 + empty-batch contract 完成** — 新 **Mixpanel destination** ([#608](https://github.com/drt-hub/drt/pull/608) by [@Pawansingh3889](https://github.com/Pawansingh3889)、`people_set` + `import_events` の 2 endpoint、EU residency、deterministic insert_id、[#417](https://github.com/drt-hub/drt/issues/417) をクローズ)。**ClickHouse `_quote_ident` 修正** ([#610](https://github.com/drt-hub/drt/pull/610) by [@yodakanohoshi](https://github.com/yodakanohoshi)) — `get_row_count` が `database.table` 形式で `Code: 62` server-side 構文エラーを引き起こしていた問題を修正、qualified-identifier 修正ファミリ (Postgres #498 / MySQL #514) の ClickHouse 版を完成。**Empty-batch contract suite 完成** (#604–#606、登録済み 25/25 destination) — `staged_upload.finalize()` の実バグを contract が発見し同 PR 内で修正。`sync.mode: mirror` ユーザー向けドキュメント (#607、postgres.md セクション + 実行可能 example + skill option)、post-#608 Mixpanel 配線 (#609)、i18n marker bump (#603) も同梱。破壊変更なし — v0.7.7 からドロップイン更新可。 |

**次のリリース:** [v0.8 Cloud Destinations & Growth](ROADMAP.md#v08--cloud-destinations--growth) → [v0.9 Enterprise Foundation](ROADMAP.md#v09--enterprise-foundation) → [v1.0 Stable Release](ROADMAP.md#v10--stable-release) → [v1.x Rust Engine](ROADMAP.md#v1x--rust-engine)

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

## CI/CD: GitHub Action

公式の [**drt-hub/drt-action**](https://github.com/drt-hub/drt-action) を使えば、CI/CD から直接 drt の同期を実行できます。インフラ不要、数行の YAML だけ。スケジュール実行・push ごと・dbt 完了直後など、好きなタイミングで起動できます。

```yaml
- uses: drt-hub/drt-action@v1
  with:
    select: '*'
    extras: postgres
  env:
    PG_PASSWORD: ${{ secrets.PG_PASSWORD }}
```

入力は同期の選択（`select`）・コネクタの `extras`・`profile`・`dry-run`・`threads` などをカバーし、出力として `status` / `succeeded` / `failed` / `duration-seconds`（＋ステップサマリ表）が得られます。シークレットは `env:` 経由で渡し、drt の `*_env` キーで解決されます。シークレットの渡し方やその他の例（dbt 後の実行、PR プレビュー）は [action README](https://github.com/drt-hub/drt-action#readme) を参照してください。

---

## エコシステム

drtは最新のデータスタックと競合するのではなく、共存するように設計されています:

<p align="center">
  <img src="docs/assets/ecosystem.png" alt="drt ecosystem — dlt load, dbt transform, drt activate" width="700">
</p>

---

## コントリビュート

typo修正から新しいコネクタの追加まで、あらゆる規模のコントリビューションを歓迎します。drt には透明性のある [コントリビューターラダー](GOVERNANCE.ja.md#ロール) があり、あなたの貢献が信頼と責任の段階的な拡大につながります。

- **始め方:** [CONTRIBUTING.ja.md](CONTRIBUTING.ja.md) — セットアップ、ワークフロー、初めてのコネクタチュートリアル
- **取り組む issue を探す:** [Good First Issues](https://github.com/drt-hub/drt/issues?q=is%3Aopen+label%3A%22good+first+issue%22)
- **意思決定の仕組みを理解する:** [GOVERNANCE.ja.md](GOVERNANCE.ja.md)
- **無料版とエンタープライズ版の違い:** [OPEN_CORE.md](OPEN_CORE.md)
- **バージョニングと破壊的変更:** [VERSIONING.md](VERSIONING.md)

## コントリビューター ✨

ご協力いただいた素晴らしい皆さまに感謝します（[絵文字キー](https://allcontributors.org/docs/en/emoji-key)）：

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://masukai.github.io/portfolio/"><img src="https://avatars.githubusercontent.com/u/37993351?v=4?s=100" width="100px;" alt="K.Masuda"/><br /><sub><b>K.Masuda</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=masukai" title="Code">💻</a> <a href="https://github.com/drt-hub/drt/commits?author=masukai" title="Documentation">📖</a> <a href="#maintenance-masukai" title="Maintenance">🚧</a> <a href="#ideas-masukai" title="Ideas, Planning, & Feedback">🤔</a> <a href="#tool-masukai" title="Tools">🔧</a> <a href="#projectManagement-masukai" title="Project Management">📆</a> <a href="https://github.com/drt-hub/drt/pulls?q=is%3Apr+reviewed-by%3Amasukai" title="Reviewed Pull Requests">👀</a> <a href="#mentoring-masukai" title="Mentoring">🧑‍🏫</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/yodakanohoshi"><img src="https://avatars.githubusercontent.com/u/25799234?v=4?s=100" width="100px;" alt="yodakanohoshi"/><br /><sub><b>yodakanohoshi</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=yodakanohoshi" title="Code">💻</a> <a href="https://github.com/drt-hub/drt/commits?author=yodakanohoshi" title="Documentation">📖</a> <a href="#maintenance-yodakanohoshi" title="Maintenance">🚧</a> <a href="#ideas-yodakanohoshi" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/drt-hub/drt/pulls?q=is%3Apr+reviewed-by%3Ayodakanohoshi" title="Reviewed Pull Requests">👀</a> <a href="#projectManagement-yodakanohoshi" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://muawiya-contact.github.io/muawiya-portfolio/"><img src="https://avatars.githubusercontent.com/u/178013839?v=4?s=100" width="100px;" alt="Moavia Amir"/><br /><sub><b>Moavia Amir</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=Muawiya-contact" title="Code">💻</a> <a href="https://github.com/drt-hub/drt/commits?author=Muawiya-contact" title="Documentation">📖</a> <a href="#infra-Muawiya-contact" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="#maintenance-Muawiya-contact" title="Maintenance">🚧</a> <a href="#ideas-Muawiya-contact" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Khush-domadia"><img src="https://avatars.githubusercontent.com/u/188820207?v=4?s=100" width="100px;" alt="Khush Domadiya"/><br /><sub><b>Khush Domadiya</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=Khush-domadia" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://pawansingh3889.github.io/"><img src="https://avatars.githubusercontent.com/u/42340841?v=4?s=100" width="100px;" alt="Pawan Singh Kapkoti"/><br /><sub><b>Pawan Singh Kapkoti</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=Pawansingh3889" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/PFCAaron12"><img src="https://avatars.githubusercontent.com/u/64714302?v=4?s=100" width="100px;" alt="PFCAaron12"/><br /><sub><b>PFCAaron12</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=PFCAaron12" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Photon101"><img src="https://avatars.githubusercontent.com/u/123910806?v=4?s=100" width="100px;" alt="Semy Ingle"/><br /><sub><b>Semy Ingle</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=Photon101" title="Code">💻</a> <a href="#maintenance-Photon101" title="Maintenance">🚧</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://kiwamizamurai.github.io/"><img src="https://avatars.githubusercontent.com/u/24860100?v=4?s=100" width="100px;" alt="きわみざむらい"/><br /><sub><b>きわみざむらい</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=kiwamizamurai" title="Code">💻</a> <a href="https://github.com/drt-hub/drt/issues?q=author%3Akiwamizamurai" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/armorbreak001"><img src="https://avatars.githubusercontent.com/u/274532465?v=4?s=100" width="100px;" alt="armorbreak001"/><br /><sub><b>armorbreak001</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=armorbreak001" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/pureqin"><img src="https://avatars.githubusercontent.com/u/213101547?v=4?s=100" width="100px;" alt="pureqin"/><br /><sub><b>pureqin</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=pureqin" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/wahajahmed010"><img src="https://avatars.githubusercontent.com/u/57330918?v=4?s=100" width="100px;" alt="Wahaj Ahmed"/><br /><sub><b>Wahaj Ahmed</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=wahajahmed010" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/cian-ps"><img src="https://avatars.githubusercontent.com/u/231972213?v=4?s=100" width="100px;" alt="cian-ps"/><br /><sub><b>cian-ps</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=cian-ps" title="Code">💻</a> <a href="https://github.com/drt-hub/drt/issues?q=author%3Acian-ps" title="Bug reports">🐛</a> <a href="#infra-cian-ps" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/xtreellaDev"><img src="https://avatars.githubusercontent.com/u/238762418?v=4?s=100" width="100px;" alt="Erik Estrella"/><br /><sub><b>Erik Estrella</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=xtreellaDev" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Ai-chan-0411"><img src="https://avatars.githubusercontent.com/u/275152799?v=4?s=100" width="100px;" alt="Ai (藍)"/><br /><sub><b>Ai (藍)</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=Ai-chan-0411" title="Documentation">📖</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/GokulKashyap"><img src="https://avatars.githubusercontent.com/u/147384970?v=4?s=100" width="100px;" alt="GokulKashyap"/><br /><sub><b>GokulKashyap</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=GokulKashyap" title="Code">💻</a> <a href="https://github.com/drt-hub/drt/commits?author=GokulKashyap" title="Tests">⚠️</a></td>
    </tr>
  </tbody>
  <tfoot>
    <tr>
      <td align="center" size="13px" colspan="7">
        <img src="https://raw.githubusercontent.com/all-contributors/all-contributors-cli/1b8533af435da9854653492b1327a23a4dbd0a10/assets/logo-small.svg">
          <a href="https://all-contributors.js.org/docs/en/bot/usage">Add your contributions</a>
        </img>
      </td>
    </tr>
  </tfoot>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

## 免責事項

drtは独立したオープンソースプロジェクトであり、dbt Labs、dlt-hub、またはその他のいかなる企業とも提携、承認、または後援関係にありません。

「dbt」はdbt Labs, Inc.の登録商標です。
「dlt」はdlt-hubによってメンテナンスされているプロジェクトです。

drtは現代のデータスタックの一部としてこれらのツールを補完するように設計されていますが、独自のコードベースとメンテナーを持つ独立したプロジェクトです。

## ライセンス

Apache 2.0 — [LICENSE](LICENSE)を参照してください。
