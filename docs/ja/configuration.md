<!-- i18n-sync: base=docs/llm/API_REFERENCE.md, hash=5be29ec57d85ab15877e4eeeb45ca13bbcf60c52 -->

[English (API Reference)](../llm/API_REFERENCE.md) | 日本語

> **Note:** この日本語リファレンスは [docs/llm/API_REFERENCE.md](../llm/API_REFERENCE.md) を基にしています。最新かつ正確な仕様は英語版を参照してください。フィールド名・型・デフォルト値は英語版と一致させています。

# drt 設定リファレンス（日本語）

drt の全設定ファイルとフィールドのリファレンスです。`drt_project.yml`・接続プロファイル・同期定義（`syncs/<name>.yml`）・デスティネーション設定・認証設定を、デフォルト値と例つきで網羅します。

各コネクタ個別の詳細は [docs/connectors/](../connectors/)、概要は [README.ja.md](../../README.ja.md) を参照してください。

## 目次

- [設定ファイルの全体像](#設定ファイルの全体像)
- [`drt_project.yml` — プロジェクト設定](#drt_projectyml--プロジェクト設定)
- [`~/.drt/profiles.yml` — 接続プロファイル（ソース）](#drtprofilesyml--接続プロファイルソース)
- [`.drt/secrets.toml` — ローカルシークレット](#drtsecretstoml--ローカルシークレット)
- [環境変数展開（`${VAR}`）](#環境変数展開var)
- [`syncs/<name>.yml` — 同期定義スキーマ](#syncsnameyml--同期定義スキーマ)
  - [トップレベルフィールド](#トップレベルフィールド)
  - [`model` の3つの書き方](#model-の3つの書き方)
  - [`sync` ブロック](#sync-ブロック)
  - [`tests` ブロック](#tests-ブロック)
- [デスティネーション設定](#デスティネーション設定)
- [認証設定（`auth`）](#認証設定auth)
- [完全な例](#完全な例)

---

## 設定ファイルの全体像

| ファイル | 役割 | コミット対象 |
|---|---|---|
| `drt_project.yml` | プロジェクト識別子・使用プロファイル・履歴設定 | ✅ Git管理 |
| `~/.drt/profiles.yml` | ソース（DWH）への接続情報 | ❌ ホームディレクトリ（コミットしない） |
| `.drt/secrets.toml` | ローカル開発用シークレット | ❌ 既定で gitignore |
| `syncs/<name>.yml` | 同期1件ぶんの定義（ソースSQL → デスティネーション） | ✅ Git管理 |

---

## `drt_project.yml` — プロジェクト設定

```yaml
name: my-project          # 必須: プロジェクト識別子
version: "0.1"            # 任意, デフォルト: "0.1"
profile: default          # 任意, デフォルト: "default" — ~/.drt/profiles.yml のキーに対応
                          # 実行時に上書き: drt run --profile prd  または  DRT_PROFILE=prd drt run
history:                  # 任意: 同期実行履歴 (#276)
  enabled: true           # デフォルト: true — false で履歴を完全に無効化
  retention_days: 30      # デフォルト: 30 — これより古いエントリは追記のたびに剪定される
```

履歴は `.drt/history/<sync_name>.jsonl`（同期ごとに1ファイル、JSONL形式）に保存されます。
`drt status --history` または MCP ツール `drt_get_history` で参照できます。

---

## `~/.drt/profiles.yml` — 接続プロファイル（ソース）

ソース（データウェアハウス）への接続情報を定義します。`type` で接続先の種類を指定します。

対応する `type`: `bigquery` | `duckdb` | `sqlite` | `postgres` | `redshift` | `clickhouse` | `snowflake` | `mysql` | `databricks` | `sqlserver`

```yaml
default:
  type: bigquery            # BigQuery
  project: my-gcp-project   # BigQuery: GCP プロジェクトID
  dataset: analytics        # BigQuery: データセット名
  location: US              # 任意: "US"（デフォルト）, "EU", "asia-northeast1" など
  method: application_default  # "application_default" | "keyfile"
  keyfile: ~/.drt/sa.json   # method=keyfile のときのみ

# DuckDB の例:
duckdb_local:
  type: duckdb
  database: ./data/local.duckdb
  dataset: main

# SQLite の例:
sqlite_local:
  type: sqlite
  database: ./data/local.db     # .sqlite/.db ファイルのパス、または ":memory:"

# PostgreSQL の例:
prod_pg:
  type: postgres
  connection_string_env: DATABASE_URL   # postgres:// URL を持つ環境変数
  dataset: public

# Redshift の例:
redshift_prod:
  type: redshift
  host: my-cluster.xxx.us-east-1.redshift.amazonaws.com
  port: 5439              # デフォルト: 5439
  dbname: analytics
  user: analyst
  password_env: REDSHIFT_PASSWORD
  schema: public          # デフォルト: "public"

# ClickHouse の例:
ch_prod:
  type: clickhouse
  host: localhost
  port: 8123              # デフォルト: 8123 (HTTP インターフェース)
  database: default
  user: default
  password_env: CLICKHOUSE_PASSWORD
```

---

## `.drt/secrets.toml` — ローカルシークレット

開発用のローカルシークレットストアです。既定で gitignore されます。

**解決順序:** YAML に直書きした値 > 環境変数 > `secrets.toml`

```toml
[destinations.mysql]
MYSQL_PASSWORD = "local-dev-password"

[destinations.github_actions]
GH_TOKEN = "ghp_xxxx"

[sources.snowflake]
SNOWFLAKE_PASSWORD = "dev-password"
```

---

## 環境変数展開（`${VAR}`）

同期 YAML の **任意の文字列フィールド**で `${VAR}` 構文が使えます（`model:` に限りません）。

```yaml
model: SELECT * FROM `${GCP_PROJECT}.${BQ_DATASET}.users`
destination:
  url: "https://${API_HOST}/api/v1/contacts"
sync:
  watermark:
    bucket: ${PIPES_GCS_BUCKET}
```

変数が未設定の場合はエラーになります。v0.6.1 以降、全文字列フィールドで対応（以前は `model:` のみ）。

---

## `syncs/<name>.yml` — 同期定義スキーマ

1件の同期（ソースSQL → デスティネーション）を定義するファイルです。完全なスキーマは次のとおりです。

```yaml
name: notify_slack          # 必須: 一意な同期識別子（ファイル名と一致させる）
description: "..."          # 任意: 人間向けの説明
model: ref('new_users')     # 必須: ref('table') | 生SQL | .sql ファイルへのパス

destination:                # 必須: 後述の「デスティネーション設定」を参照
  type: rest_api
  # ... デスティネーション固有のフィールド

sync:                       # 任意: 全フィールドにデフォルトあり
  mode: full                # "full"（デフォルト） | "incremental" | "upsert" | "replace" | "mirror"
  mirror:                   # 任意 (#686, #687): mirror モードの削除挙動 — mode: mirror のときのみ有効
    strategy: destination   # "destination"（デフォルト） | "tracked" — 詳細は下記「mirror.strategy」
    scope: [parent_id]      # 任意 (#687): 削除対象を「今回の実行で観測した scope カラム値を持つ行」に限定 — 詳細は下記
  cursor_field: updated_at  # mode=incremental のとき必須 — ウォーターマーク用のカラム名
  watermark:                # 任意: ステートレス環境向けのリモートウォーターマーク保存
    storage: local          # "local"（デフォルト） | "gcs" | "bigquery"
    bucket: my-bucket       # gcs のみ
    key: watermarks/s.json  # gcs のみ
    project: my-project     # bigquery のみ
    dataset: my_dataset     # bigquery のみ
    default_value: "2026-01-01 00:00:00"  # 任意: 初回実行時のフォールバックカーソル (v0.6.2)
  batch_size: 100           # デフォルト: 100 — デスティネーション1回あたりの行数
  on_error: fail            # "fail"（デフォルト） | "skip"
  field_mappings:           # 任意 (#415): 宣言的なカラムリネーム {ソースカラム: デスティネーションフィールド}
    user_id: id             # 抽出・カーソル追跡・lookups の後、デスティネーション直前に適用
    full_name: name         # cursor_field / lookups はソース名、upsert_key / デスティネーションカラムはマッピング後の名前を使う
  dlq:                      # 任意 (#278): Dead Letter Queue — レコード単位の load 失敗を保存して再送可能にする
    enabled: false          # デフォルト: false（オプトイン） — 失敗レコード全体を .drt/dlq/<sync>.jsonl に書く（PII に注意）
    max_records: 10000      # デフォルト: 10000 — キュー上限。超過分は古いものから破棄（0 = 無制限）
  rate_limit:
    requests_per_second: 10 # デフォルト: 10 — 0 でレート制限を無効化
  retry:                    # sync レベルの retry（デスティネーション側で上書きしない限り適用）
    max_attempts: 3         # デフォルト: 3
    initial_backoff: 1.0    # デフォルト: 1.0 秒
    backoff_multiplier: 2.0 # デフォルト: 2.0 — 1.0 にすると線形（一定）バックオフ
    max_backoff: 60.0       # デフォルト: 60.0 秒
    retryable_status_codes: [429, 500, 502, 503, 504]  # デフォルトは左記のとおり

tests:                      # 任意: 同期後の検証（DBデスティネーションのみ）
  - row_count:
      min: 1
      max: 10000
  - not_null:
      columns: [id, name]
```

### トップレベルフィールド

| フィールド | 必須 | 説明 |
|---|---|---|
| `name` | ✅ | 一意な同期識別子。ファイル名と一致させる |
| `description` | | 人間向けの説明 |
| `model` | ✅ | 抽出するデータ。`ref('table')` / 生SQL / `.sql` ファイルパス |
| `destination` | ✅ | 送信先。[デスティネーション設定](#デスティネーション設定)を参照 |
| `sync` | | 同期の挙動。全フィールドにデフォルトあり |
| `tests` | | 同期後の検証（DBデスティネーションのみ） |

### `model` の3つの書き方

- `ref('table_name')` — dbt 風の参照。プロファイルのデータセット内のテーブルを指す
- 生SQL — `SELECT ... FROM ...` を直接記述
- `.sql` ファイルへのパス — SQL を別ファイルに切り出す

### `sync` ブロック

#### `mode` — 同期モード

| モード | 挙動 |
|---|---|
| `full`（デフォルト） | 毎回ソース全件を送信。`upsert_key` が設定されていれば upsert 相当 |
| `incremental` | `cursor_field` を使い、前回以降に増分した行のみ送信 |
| `upsert` | `upsert_key` が設定された `full` のエイリアス |
| `replace` | デスティネーションを `TRUNCATE` してから `INSERT` |
| `mirror` | upsert したうえで、ソースに存在しない `upsert_key` の行をデスティネーションから `DELETE`（#340 — Postgres / MySQL / ClickHouse / Snowflake） |

#### `mirror.strategy` — mirror の削除挙動（#686）

| strategy | 挙動 | 使いどころ |
|---|---|---|
| `destination`（デフォルト） | **宛先テーブル全体**との diff で削除を決定（#340） | drt がテーブルを専有している場合 |
| `tracked` | **drt 自身が同期したキー集合**（宛先内の `_drt_synced_keys` テーブルで sync ごとに追跡）との diff で削除。アプリが書いた行は削除候補にならない | アプリも書き込む共有テーブル（Reverse ETL の典型ケース） |

`tracked` の挙動: 初回実行は baseline のみ（削除なし）。state 喪失時は WARN を出して再 baseline。ターゲットの DELETE と state の書き換えは同一トランザクション。int / str のキーは正確に round-trip し、それ以外の型は文字列化されます（既知の制限）。現状 **Postgres / MySQL のみ**対応（ClickHouse / Snowflake / Databricks は明示的なエラーで拒否）。

**`mirror.scope`（#687）** — 1:N 再生成（親エンティティ + 子リンク行）向けのステートレスな削除制限。`scope: [parent_id]` を指定すると、DELETE は「**今回観測した親**に属し、かつ今回のキー集合に無い行」だけになります（`WHERE parent_id IN (観測した親) AND upsert_key NOT IN (観測したキー)`）。観測していない親の行（他パイプラインやアプリが書いた行）には触れません。scope カラムがモデル出力に無い場合は書き込み前にエラー。複合 scope 可。`strategy: tracked` との併用は現状不可（follow-up）。Postgres / MySQL のみ対応。

#### `cursor_field` / `watermark`

- `cursor_field`: `mode=incremental` のとき必須。ウォーターマークに使うカラム名
- `watermark.storage`: ウォーターマークの保存先。`local`（デフォルト）/ `gcs` / `bigquery`。CI などステートレスな環境では `gcs`・`bigquery` を使ってリモートに保持できる
- `watermark.default_value`: 初回実行時のフォールバックカーソル値（v0.6.2）

#### その他の `sync` フィールド

- `batch_size`（デフォルト 100）: デスティネーション1回の呼び出しあたりの行数
- `on_error`（デフォルト `fail`）: `fail`（即エラー）/ `skip`（該当行をスキップして継続）
- `field_mappings`（#415）: `{ソースカラム: デスティネーションフィールド}` の宣言的リネーム。抽出・カーソル追跡・lookups の **後**、デスティネーション送信の直前に適用される。`cursor_field` / `lookups` は**ソース名**、`upsert_key` / デスティネーションカラムは**マッピング後の名前**で指定する点に注意
- `dlq`（#278）: Dead Letter Queue。`enabled: true` でレコード単位の load 失敗を `.drt/dlq/<sync>.jsonl` に保存し、後で再送できる。失敗レコード全体を書き出すため PII の取り扱いに注意。`max_records`（デフォルト 10000）で上限管理（0 = 無制限）
- `rate_limit.requests_per_second`（デフォルト 10）: 0 でレート制限を無効化

#### `retry` の優先順位

retry はリトライ戦略を制御します。優先順位は次のとおりです（#277）:

```
destination.retry > sync.retry > RetryConfig のデフォルト
```

つまり HTTP 系デスティネーションの設定ブロック内に `retry:` を書くと、その**デスティネーションだけ** `sync.retry` を上書きできます。

```yaml
sync:
  retry:
    max_attempts: 3

destination:
  type: notion
  retry:
    max_attempts: 7       # このデスティネーションだけ 7 回リトライ
```

| フィールド | デフォルト | 説明 |
|---|---|---|
| `max_attempts` | 3 | 最大試行回数 |
| `initial_backoff` | 1.0 | 初回バックオフ（秒） |
| `backoff_multiplier` | 2.0 | バックオフ倍率。1.0 で線形（一定） |
| `max_backoff` | 60.0 | バックオフ上限（秒） |
| `retryable_status_codes` | `[429, 500, 502, 503, 504]` | リトライ対象の HTTP ステータス |

### `tests` ブロック

同期後の検証です（DBデスティネーションのみ）。

```yaml
tests:
  - row_count:
      min: 1                # 任意: 期待する最小行数
      max: 10000            # 任意: 期待する最大行数
  - not_null:
      columns: [id, name]   # 必須: NULL を含んではならないカラム
  - freshness:
      column: updated_at    # 必須: チェック対象のタイムスタンプカラム
      max_age: "7 days"     # 必須: 人間可読な最大経過時間（"24 hours", "7 days" など）
  - unique:
      columns: [id]         # 必須: 一意でなければならないカラム
  - accepted_values:
      column: status        # 必須: チェック対象のカラム
      values: [active, inactive, pending]  # 必須: 許可される値
```

---

## デスティネーション設定

`destination.type` で送信先を指定します。各タイプ固有のフィールドは以下のとおりです。Jinja2 テンプレート（`*_template`）では `{{ row.<column> }}` で行の値を参照できます。

### `type: rest_api`

```yaml
destination:
  type: rest_api
  url: "https://hooks.example.com/webhook"   # 必須
  method: POST                               # "GET"|"POST"|"PUT"|"PATCH"|"DELETE", デフォルト: POST
  headers:                                   # 任意: dict
    Content-Type: "application/json"
    X-Custom-Header: "value"
  body_template: |                           # 任意: Jinja2 テンプレート → リクエストボディ
    {
      "user_id": "{{ row.id }}",
      "email": "{{ row.email }}"
    }
  auth:                                      # 任意 — 「認証設定」を参照
    type: bearer
    token_env: MY_API_TOKEN
```

### `type: slack`

```yaml
destination:
  type: slack
  webhook_url: "https://hooks.slack.com/..."   # webhook_url か webhook_url_env のどちらか
  webhook_url_env: SLACK_WEBHOOK_URL           # 環境変数名
  message_template: "New user: {{ row.name }} ({{ row.email }})"  # Jinja2, デフォルト: "{{ row }}"
  block_kit: false                             # true で message_template を Block Kit JSON として扱う
```

### `type: discord`

```yaml
destination:
  type: discord
  webhook_url: "https://discord.com/api/webhooks/..."  # webhook_url か webhook_url_env
  webhook_url_env: DISCORD_WEBHOOK_URL                 # 環境変数名
  message_template: "New user: {{ row.name }} ({{ row.email }})"  # Jinja2, デフォルト: "{{ row }}"
  embeds: false                                        # true で message_template を embeds JSON として扱う
```

### `type: github_actions`

```yaml
destination:
  type: github_actions
  owner: myorg                    # 必須: GitHub org またはユーザー
  repo: myapp                     # 必須: リポジトリ名
  workflow_id: deploy.yml         # 必須: ワークフローのファイル名または数値ID
  ref: main                       # デフォルト: "main" — 実行対象のブランチ/タグ
  inputs_template: |              # 任意: Jinja2 テンプレート → workflow inputs の JSON オブジェクト
    {
      "environment": "{{ row.env }}",
      "version": "{{ row.version }}"
    }
  auth:
    type: bearer
    token_env: GITHUB_TOKEN       # actions:write 権限が必要
```

### `type: hubspot`

```yaml
destination:
  type: hubspot
  object_type: contacts           # "contacts" | "deals" | "companies", デフォルト: "contacts"
  id_property: email              # デフォルト: "email" — upsert の重複判定キー
  properties_template: |          # 任意: Jinja2 テンプレート → HubSpot プロパティの JSON オブジェクト
    {
      "email": "{{ row.email }}",
      "firstname": "{{ row.first_name }}",
      "lastname": "{{ row.last_name }}",
      "company": "{{ row.company }}"
    }
  auth:
    type: bearer
    token_env: HUBSPOT_TOKEN      # CRM 書き込みスコープを持つ Private App トークン
```

### `type: zendesk`

```yaml
destination:
  type: zendesk
  subdomain_env: ZENDESK_SUBDOMAIN       # 例: acme.zendesk.com の "acme"
  email_env: ZENDESK_EMAIL               # Zendesk ユーザーのメール
  api_token_env: ZENDESK_API_TOKEN       # Zendesk API トークン
  object: user                           # "user"（デフォルト） | "organization"
  id_field: zendesk_user_id              # 任意: Zendesk の id にコピーするソースフィールド
  custom_fields_template: |              # 任意: カスタムフィールドの JSON オブジェクト
    {
      "health_score": "{{ row.health_score }}",
      "plan": "{{ row.plan }}"
    }
```

> ユーザーは `users/create_or_update_many` で100件ずつバッチ upsert されます。Organization は `organizations/create_or_update` で1行ずつ処理されます。カスタムフィールドは `user_fields` / `organization_fields` として送信されます。

### `type: jira`

```yaml
destination:
  type: jira
  base_url_env: JIRA_BASE_URL           # 環境変数 → 例: https://myorg.atlassian.net
  email_env: JIRA_EMAIL                 # 環境変数 → Jira アカウントのメール
  token_env: JIRA_API_TOKEN             # 環境変数 → Jira API トークン
  project_key: "PROJ"                   # Jira プロジェクトキー（Jinja2 可）
  issue_type: "Task"                    # デフォルト: "Task"（Jinja2 可）
  summary_template: "Alert: {{ row.title }}"         # 必須: Jinja2 テンプレート
  description_template: "Details: {{ row.body }}"    # 必須: Jinja2 テンプレート
  issue_id_field: issue_id              # デフォルト: "issue_id" — 行に存在すれば更新、なければ新規作成
```

> **作成と更新の切り替え:** 行に `issue_id_field`（デフォルト `issue_id`）カラムが含まれていればその Jira issue を更新（PUT）、なければ新規作成（POST）します。description は Jira REST API v3 向けに Atlassian Document Format (ADF) でレンダリングされます。

### `type: google_sheets`

```yaml
destination:
  type: google_sheets
  spreadsheet_id: "1BxiMVs0XRA5nFMd..."   # 必須: URL から取得する Google Sheets ID
  sheet: "Sheet1"                           # デフォルト: "Sheet1"
  mode: overwrite                           # "overwrite"（デフォルト） | "append"
  credentials_path: /path/to/sa-key.json   # サービスアカウント JSON キーファイル
  credentials_env: GOOGLE_SA_KEY_PATH      # または: キーファイルパスを指す環境変数
```

> `overwrite` はシートをクリアしてヘッダー+データ行を書き込みます。`append` はデータ行のみ追記します。

### `type: postgres`（デスティネーション）

```yaml
# 方式A: 環境変数の接続文字列
destination:
  type: postgres
  connection_string_env: DATABASE_URL  # postgres://user:pass@host:5432/dbname を持つ環境変数
  table: public.analytics_scores       # 必須: 対象テーブル
  upsert_key: [id]                     # 必須: ON CONFLICT に使うカラム

# 方式B: 個別パラメータ
destination:
  type: postgres
  host_env: TARGET_PG_HOST           # ホストの環境変数（または host:）
  port: 5432                         # デフォルト: 5432
  dbname_env: TARGET_PG_DBNAME       # データベース名の環境変数
  user_env: TARGET_PG_USER           # ユーザーの環境変数
  password_env: TARGET_PG_PASSWORD   # パスワードの環境変数
  table: public.analytics_scores     # 必須: 対象テーブル
  upsert_key: [id]                   # 必須: ON CONFLICT に使うカラム
  ssl:                               # 任意: SSL/TLS 接続
    enabled: true
    ca_env: PG_SSL_CA                # CA 証明書パスの環境変数
    cert_env: PG_SSL_CERT            # クライアント証明書パスの環境変数
    key_env: PG_SSL_KEY              # クライアント鍵パスの環境変数
```

> `INSERT ... ON CONFLICT (upsert_key) DO UPDATE SET ...` で冪等な書き込みを行います。
> `connection_string_env` と個別パラメータの両方がある場合は `connection_string_env` が優先されます。

### `type: mysql`

```yaml
# 方式A: 環境変数の接続文字列
destination:
  type: mysql
  connection_string_env: MYSQL_URL     # mysql://user:pass@host:3306/dbname を持つ環境変数
  table: analytics.scores              # 必須: 対象テーブル
  upsert_key: [id]                     # 必須: ON DUPLICATE KEY に使うカラム

# 方式B: 個別パラメータ
destination:
  type: mysql
  host_env: TARGET_MYSQL_HOST        # ホストの環境変数
  port: 3306                         # デフォルト: 3306
  database_env: TARGET_MYSQL_DB      # データベースの環境変数
  user_env: TARGET_MYSQL_USER        # ユーザーの環境変数
  password_env: TARGET_MYSQL_PASS    # パスワードの環境変数
  table: analytics.scores            # 必須: 対象テーブル
  upsert_key: [id]                   # 必須: ON DUPLICATE KEY に使うカラム
  ssl:                               # 任意: SSL/TLS 接続
    enabled: true
    ca_env: MYSQL_SSL_CA
    cert_env: MYSQL_SSL_CERT
    key_env: MYSQL_SSL_KEY
```

> `INSERT ... ON DUPLICATE KEY UPDATE ...` で冪等な書き込みを行います。
> `connection_string_env` と個別パラメータの両方がある場合は `connection_string_env` が優先されます。

### `type: clickhouse`（デスティネーション）

```yaml
destination:
  type: clickhouse
  host: localhost                      # または host_env
  port: 8123                           # デフォルト: 8123 (HTTP)
  database: default                    # 必須
  user: default                        # または user_env
  password_env: CH_PASSWORD            # パスワードの環境変数
  table: analytics.scores             # 必須: 対象テーブル
  upsert_key: [id]                     # 任意: ReplacingMergeTree による重複排除
  secure: false                        # true で HTTPS
  connection_string_env: CH_CONN       # 代替: 接続文字列全体
```

### `lookups`（DBデスティネーション: postgres / mysql / clickhouse）

同期中にデスティネーションDBを問い合わせて、外部キー（FK）の値を解決します。全DBデスティネーションタイプで利用できます。

```yaml
destination:
  type: mysql                          # または postgres, clickhouse
  # ... 接続フィールド ...
  table: child_table
  upsert_key: [parent_id, code]
  lookups:                             # 任意: デスティネーションDB経由の FK 解決
    parent_id:                         # デスティネーション側で埋めるカラム
      table: parent_table              # 問い合わせ先のデスティネーションDBテーブル
      match:                           # { デスティネーションカラム: ソースカラム }
        user_id: user_id
      select: id                       # lookup テーブルから取得するカラム
      on_miss: skip                    # "skip"（デフォルト） | "fail" | "null"
```

- **`table`**（必須）: 問い合わせ先のデスティネーションDBテーブル
- **`match`**（必須）: `{ デスティネーションカラム: ソースカラム }` のマッピング。複合キー可
- **`select`**（必須）: lookup テーブルから取得するカラム
- **`on_miss`**（任意, デフォルト `"skip"`）:
  - `skip` — その行をスキップし警告ログを出す
  - `fail` — エラー扱い（`sync.on_error` に従う）
  - `null` — 対象カラムを NULL にする
- **`drop_match_columns`**（任意, デフォルト `true`）: FK 解決後に match のソースカラムを INSERT から除外する。match カラムがデスティネーションテーブルにも存在する場合は `false` に設定する

1つの同期に複数の lookups を定義できます。各 lookup はバッチループの前に1回 SELECT を実行します。

### `type: teams`

```yaml
destination:
  type: teams
  webhook_url_env: TEAMS_WEBHOOK_URL   # Incoming Webhook URL の環境変数
  message_template: "New alert: {{ row.message }}"  # Jinja2 プレーンテキスト
  adaptive_card: false                 # true で message_template を Adaptive Card JSON として扱う
```

### `type: parquet`

```yaml
destination:
  type: parquet
  path: output/data.parquet            # 必須: 出力ファイルパス
  compression: snappy                  # "snappy"（デフォルト） | "gzip" | "zstd" | "none"
  partition_by: [region, date]         # 任意: パーティションカラム
```

> 要インストール: `pip install drt-core[parquet]`

### `type: file`

```yaml
destination:
  type: file
  path: output/data.csv               # 必須: 出力ファイルパス
  format: csv                          # "csv" | "json" | "jsonl"
```

> 追加依存なし — 標準ライブラリの csv / json を使用します。

### `type: linear`

```yaml
destination:
  type: linear
  token_env: LINEAR_API_KEY            # Linear API キーの環境変数
  team_id: "TEAM-ID"                   # 必須: Linear チームID
  title_template: "{{ row.title }}"    # issue タイトルの Jinja2 テンプレート
  description_template: "{{ row.body }}"  # 説明の Jinja2 テンプレート
```

### `type: sendgrid`

```yaml
destination:
  type: sendgrid
  api_key_env: SENDGRID_API_KEY        # SendGrid API キーの環境変数
  from_email: alerts@example.com       # 必須: 送信元メール
  to_field: email                      # 宛先メールに使う行フィールド
  subject_template: "Alert: {{ row.title }}"  # Jinja2 テンプレート
  body_template: "{{ row.message }}"   # メール本文の Jinja2 テンプレート
```

### `type: google_ads`

```yaml
destination:
  type: google_ads
  customer_id: "1234567890"            # 必須: Google Ads カスタマーID（ハイフンなし）
  conversion_action: "customers/1234567890/conversionActions/987"  # 必須
  gclid_field: gclid                   # クリックIDの行フィールド（デフォルト: "gclid"）
  conversion_time_field: conversion_time  # タイムスタンプの行フィールド
  conversion_value_field: revenue      # 任意: コンバージョン値の行フィールド
  currency_code: JPY                   # デフォルト: USD
  developer_token_env: GOOGLE_ADS_DEVELOPER_TOKEN
  auth:
    type: oauth2_client_credentials
    token_url: "https://oauth2.googleapis.com/token"
    client_id_env: GOOGLE_ADS_CLIENT_ID
    client_secret_env: GOOGLE_ADS_CLIENT_SECRET
```

### `type: staged_upload`

ファイルアップロード → ジョブ起動 → 完了ポーリング、という流れの API 向け（例: Amazon Marketing Cloud, Salesforce Bulk API 2.0）。

```yaml
destination:
  type: staged_upload
  format: csv                          # "csv" | "json" | "jsonl"
  stage:
    url: "https://upload.example.com/files"
    method: POST
    auth:
      type: bearer
      token_env: API_TOKEN
    response_extract:
      upload_id: "uploadId"            # レスポンス JSON から抽出
  trigger:
    url: "https://api.example.com/jobs"
    method: POST
    body_template: '{"uploadId": "{{ upload_id }}"}'
    auth:
      type: bearer
      token_env: API_TOKEN
    response_extract:
      job_id: "jobId"
  poll:                                # 任意 — 省略すると fire-and-forget
    url: "https://api.example.com/jobs/{{ job_id }}"
    method: GET
    auth:
      type: bearer
      token_env: API_TOKEN
    status_field: "status"
    success_values: ["SUCCEEDED"]
    failure_values: ["FAILED"]
    interval_seconds: 30               # デフォルト: 30
    timeout_seconds: 3600              # デフォルト: 3600
```

> ここに記載のないデスティネーション（Notion / Twilio / Intercom / Email SMTP / Salesforce / Amplitude / Mixpanel / Klaviyo / Airtable / Snowflake / BigQuery / Databricks / S3 / GCS / Azure / Elasticsearch など）は [docs/connectors/](../connectors/) を参照してください。

---

## 認証設定（`auth`）

認証設定はデスティネーション設定内の `auth:` キーの下で使います。

### Bearer トークン

```yaml
auth:
  type: bearer
  token_env: MY_TOKEN     # 推奨: トークンを格納した環境変数名
  token: "sk-..."         # 非推奨: ハードコードしたトークン（token_env を使うこと）
```

→ `Authorization: Bearer <token>` ヘッダーを送信します。

### API キー

```yaml
auth:
  type: api_key
  header: X-API-Key       # デフォルト: "X-API-Key" — ヘッダー名
  value_env: MY_API_KEY   # 推奨: 環境変数名
  value: "abc123"         # 非推奨: ハードコードした値
```

→ `<header>: <value>` ヘッダーを送信します。

### Basic 認証

```yaml
auth:
  type: basic
  username_env: API_USERNAME   # 必須: 環境変数名
  password_env: API_PASSWORD   # 必須: 環境変数名
```

→ `Authorization: Basic <base64(username:password)>` ヘッダーを送信します。

### OAuth2 Client Credentials

```yaml
auth:
  type: oauth2_client_credentials
  token_url: "https://auth.example.com/oauth/token"  # 必須
  client_id_env: OAUTH_CLIENT_ID       # 必須: 環境変数名
  client_secret_env: OAUTH_CLIENT_SECRET  # 必須: 環境変数名
  scope: "contacts.write"             # 任意
```

→ クライアント認証情報をアクセストークンに交換し、有効期限までキャッシュします。`Authorization: Bearer <access_token>` ヘッダーを送信します。

---

## 完全な例

### Slack 通知 — incremental

```yaml
name: new_user_slack
description: "新規ユーザー登録時に Slack へ通知"
model: ref('users')

destination:
  type: slack
  webhook_url_env: SLACK_WEBHOOK_URL
  message_template: ":wave: New user: *{{ row.name }}* ({{ row.email }})"

sync:
  mode: incremental
  cursor_field: created_at
  batch_size: 50
  on_error: skip
  rate_limit:
    requests_per_second: 5
```

### HubSpot コンタクト upsert — full

```yaml
name: sync_contacts_hubspot
description: "HubSpot のコンタクトを DWH と同期し続ける"
model: ref('active_customers')

destination:
  type: hubspot
  object_type: contacts
  id_property: email
  properties_template: |
    {
      "email": "{{ row.email }}",
      "firstname": "{{ row.first_name }}",
      "lastname": "{{ row.last_name }}",
      "company": "{{ row.company_name }}",
      "lifecyclestage": "customer"
    }
  auth:
    type: bearer
    token_env: HUBSPOT_TOKEN

sync:
  mode: full
  batch_size: 100
  on_error: skip
  retry:
    max_attempts: 5
    initial_backoff: 2.0
```

### PostgreSQL upsert — incremental

```yaml
name: sync_scores
description: "分析スコアを対象 Postgres に upsert"
model: ref('user_scores')

destination:
  type: postgres
  host_env: TARGET_PG_HOST
  dbname_env: TARGET_PG_DBNAME
  user_env: TARGET_PG_USER
  password_env: TARGET_PG_PASSWORD
  table: public.analytics_scores
  upsert_key: [user_id]

sync:
  mode: incremental
  cursor_field: updated_at
  on_error: skip
```

### REST API — カスタム認証ヘッダー

```yaml
name: push_to_webhook
model: ref('events')

destination:
  type: rest_api
  url: "https://api.example.com/events"
  method: POST
  headers:
    Content-Type: "application/json"
  body_template: |
    {
      "event_id": "{{ row.id }}",
      "type": "{{ row.event_type }}",
      "occurred_at": "{{ row.created_at }}"
    }
  auth:
    type: api_key
    header: X-API-Key
    value_env: EXAMPLE_API_KEY

sync:
  batch_size: 50
  rate_limit:
    requests_per_second: 20
  retry:
    max_attempts: 3
    retryable_status_codes: [429, 500, 502, 503, 504]
  on_error: skip
```

---

## 関連ドキュメント

- [docs/llm/API_REFERENCE.md](../llm/API_REFERENCE.md) — 英語版の完全リファレンス（本書のベース）
- [docs/connectors/](../connectors/) — コネクタ個別の詳細リファレンス
- [README.ja.md](../../README.ja.md) — プロジェクト概要・クイックスタート
- [docs/guides/](../guides/) — 機能別ガイド（DLQ・retry・field-mappings・dry-run など）
