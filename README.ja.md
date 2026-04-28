<!-- i18n-sync: base=README.md, hash=a81922aeba9795158b43afb86a38886d2259a5a2 -->

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
| `drt_run_sync` | 同期を実行（`dry_run`対応） |
| `drt_get_status` | 前回の実行結果を取得 |
| `drt_validate` | 同期YAML設定を検証 |
| `drt_get_schema` | 設定ファイルのJSONスキーマを返す |
| `drt_list_connectors` | 利用可能なソースとデスティネーションを一覧 |

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

### デスティネーション

| コネクタ | ステータス | インストール | 認証 |
|-----------|--------|---------|------|
| REST API | ✅ v0.1 | (core) | Bearer / API Key / Basic / OAuth2 |
| Slack Incoming Webhook | ✅ v0.1 | (core) | Webhook URL |
| Discord Webhook | ✅ v0.4.2 | (core) | Webhook URL |
| GitHub Actions | ✅ v0.1 | (core) | Token（環境変数） |
| HubSpot | ✅ v0.1 | (core) | Token（環境変数） |
| Google Ads | ✅ v0.6 | (core) | OAuth2 Client Credentials |
| Google Sheets | ✅ v0.4 | `pip install drt-core[sheets]` | Service Account Keyfile |
| PostgreSQL (upsert) | ✅ v0.4 | `pip install drt-core[postgres]` | パスワード（環境変数） |
| MySQL (upsert) | ✅ v0.4 | `pip install drt-core[mysql]` | パスワード（環境変数） |
| ClickHouse | ✅ v0.5 | `pip install drt-core[clickhouse]` | パスワード（環境変数） |
| Parquet file | ✅ v0.5 | `pip install drt-core[parquet]` | ファイルパス |
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

**次のリリース:** [v0.7 Production Ready](ROADMAP.md#v07--production-ready) → [v0.8 Cloud Destinations & Growth](ROADMAP.md#v08--cloud-destinations--growth) → [v0.9 Enterprise Foundation](ROADMAP.md#v09--enterprise-foundation) → [v1.0 Stable Release](ROADMAP.md#v10--stable-release) → [v1.x Rust Engine](ROADMAP.md#v1x--rust-engine)

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

typo修正から新しいコネクタの追加まで、あらゆる規模のコントリビューションを歓迎します。drt には透明性のある [コントリビューターラダー](GOVERNANCE.ja.md#ロール) があり、あなたの貢献が信頼と責任の段階的な拡大につながります。

- **始め方:** [CONTRIBUTING.ja.md](CONTRIBUTING.ja.md) — セットアップ、ワークフロー、初めてのコネクタチュートリアル
- **取り組む issue を探す:** [Good First Issues](https://github.com/drt-hub/drt/issues?q=is%3Aopen+label%3A%22good+first+issue%22)
- **意思決定の仕組みを理解する:** [GOVERNANCE.ja.md](GOVERNANCE.ja.md)

## Contributors ✨

このプロジェクトでは [emoji key](https://allcontributors.org/docs/en/emoji-key) に基づいて、素晴らしい貢献者のみなさんを紹介しています。

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://masukai.github.io/portfolio/"><img src="https://avatars.githubusercontent.com/u/37993351?v=4&s=100" width="100px;" alt="K.Masuda"/><br /><sub><b>K.Masuda</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=masukai" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.youtube.com/@Coding_Moves"><img src="https://avatars.githubusercontent.com/u/178013839?v=4&s=100" width="100px;" alt="Moavia Amir"/><br /><sub><b>Moavia Amir</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=Muawiya-contact" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Khush-domadia"><img src="https://avatars.githubusercontent.com/u/188820207?v=4&s=100" width="100px;" alt="Khush Domadiya"/><br /><sub><b>Khush Domadiya</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=Khush-domadia" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://pawansingh3889.github.io/"><img src="https://avatars.githubusercontent.com/u/42340841?v=4&s=100" width="100px;" alt="Pawan Singh Kapkoti"/><br /><sub><b>Pawan Singh Kapkoti</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=Pawansingh3889" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/PFCAaron12"><img src="https://avatars.githubusercontent.com/u/64714302?v=4&s=100" width="100px;" alt="PFCAaron12"/><br /><sub><b>PFCAaron12</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=PFCAaron12" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/armorbreak001"><img src="https://avatars.githubusercontent.com/u/274532465?v=4&s=100" width="100px;" alt="armorbreak001"/><br /><sub><b>armorbreak001</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=armorbreak001" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/pureqin"><img src="https://avatars.githubusercontent.com/u/213101547?v=4&s=100" width="100px;" alt="pureqin"/><br /><sub><b>pureqin</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=pureqin" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/wahajahmed010"><img src="https://avatars.githubusercontent.com/u/57330918?v=4&s=100" width="100px;" alt="Wahaj Ahmed"/><br /><sub><b>Wahaj Ahmed</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=wahajahmed010" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/cian-ps"><img src="https://avatars.githubusercontent.com/u/231972213?v=4&s=100" width="100px;" alt="cian-ps"/><br /><sub><b>cian-ps</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=cian-ps" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/xtreellaDev"><img src="https://avatars.githubusercontent.com/u/238762418?v=4&s=100" width="100px;" alt="Erik Estrella"/><br /><sub><b>Erik Estrella</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=xtreellaDev" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Ai-chan-0411"><img src="https://avatars.githubusercontent.com/u/275152799?v=4&s=100" width="100px;" alt="Ai (藍)"/><br /><sub><b>Ai (藍)</b></sub></a><br /><a href="https://github.com/drt-hub/drt/commits?author=Ai-chan-0411" title="Code">💻</a></td>
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

このプロジェクトは [all-contributors](https://github.com/all-contributors/all-contributors) 仕様に従っています。あらゆる種類の貢献を歓迎します。

## 免責事項

drtは独立したオープンソースプロジェクトであり、dbt Labs、dlt-hub、またはその他のいかなる企業とも提携、承認、または後援関係にありません。

「dbt」はdbt Labs, Inc.の登録商標です。
「dlt」はdlt-hubによってメンテナンスされているプロジェクトです。

drtは現代のデータスタックの一部としてこれらのツールを補完するように設計されていますが、独自のコードベースとメンテナーを持つ独立したプロジェクトです。

## ライセンス

Apache 2.0 — [LICENSE](LICENSE)を参照してください。
