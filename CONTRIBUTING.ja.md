<!-- i18n-sync: base=CONTRIBUTING.md, hash=8991d51e967280adc48c8a2b507a9f929ce67a01 -->

[English](./CONTRIBUTING.md) | [日本語](./CONTRIBUTING.ja.md)

> **Note:** この翻訳は最新でない可能性があります。正確な情報は [CONTRIBUTING.md](CONTRIBUTING.md) を参照してください。

# drt への貢献

ご協力いただきありがとうございます！

## 開発環境のセットアップ

### 前提条件

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)（推奨）または pip

### クローンとインストール

```bash
git clone https://github.com/drt-hub/drt.git
cd drt
```

**uv を使う場合（推奨）：**

```bash
uv sync --extra dev --extra bigquery
```

**pip を使う場合：**

```bash
pip install -e ".[dev,bigquery]"
```

または Makefile のショートカット：

```bash
make dev
```

## テストの実行

```bash
make test       # すべてのテストを実行（pytest）
make lint       # ruff + mypy
make fmt        # 自動フォーマット（ruff format + fix）
```

コマンドを直接実行することもできます：

```bash
uv run pytest
uv run ruff check drt tests
uv run mypy drt
```

## ブランチ命名規則

| プレフィックス | 用途 |
|--------|-------------|
| `feat/` | 新機能やコネクタの追加 |
| `fix/` | バグ修正 |
| `docs/` | ドキュメントの変更 |
| `chore/` | メンテナンス、依存関係の更新、CI の変更 |

例: `feat/snowflake-source`, `fix/empty-batch-rest-api`, `docs/quickstart-update`

## ブランチ戦略

drt は **GitHub Flow** を使用します — すべての開発はフィーチャーブランチで行い、`main` に直接マージします。

- `main` は常にリリース可能な状態です
- `develop` や `release` ブランチはありません
- リリースはタグ（`v0.2.0`, `v0.3.0`, ...）でマークされます

## コミット署名（必須）

`main` ブランチではサプライチェーン攻撃対策として**署名付きコミット**が必須です。すべての PR は **Squash & merge** でマージされ、GitHub が自動的にスカッシュコミットに署名するため、**コントリビューターが署名を設定しなくても貢献できます**。

ただし、保護ブランチへの直接プッシュや、コミットに「Verified」バッジを表示したい場合は、SSH 署名を設定してください：

```bash
# 既存の SSH 鍵を使用（なければ生成: ssh-keygen -t ed25519）
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/id_ed25519.pub
git config --global commit.gpgsign true
```

同じ鍵を [GitHub の SSH 設定](https://github.com/settings/keys)で **Signing Key** として追加してください。

## 変更の提出

1. リポジトリをフォークする
2. 上記の命名規則に従ってブランチを作成する: `git checkout -b feat/your-feature`
3. テストを含む変更を加える
4. `make lint` と `make test` を実行してすべてがパスすることを確認する
5. プルリクエストを開き、PR テンプレートに記入する

> **マージ戦略:** すべての PR は **Squash & merge** でマージされます。ブランチのコミットは `main` 上の単一のコミットにスカッシュされるため、WIP コミットをクリーンアップする必要はありません。GitHub がスカッシュコミットに自動署名します。

## プルリクエストチェックリスト

- [ ] テストがパスする（`make test`）
- [ ] リンターがパスする（`make lint`）
- [ ] `CHANGELOG.md` が更新されている（ユーザー向けの変更の場合）
- [ ] 新しいコネクタには `tests/` 配下のテストと `examples/` 配下の例が含まれている

## コミットスタイル

[Conventional Commits](https://www.conventionalcommits.org/) を使用してください：

```
feat: add Snowflake source
fix: handle empty batch in REST API destination
docs: update quickstart example
chore: bump dependencies
```

## コネクタの追加

プロトコルのインターフェースについては `drt/sources/base.py` と `drt/destinations/base.py` を参照してください。
プロトコルを実装し、`tests/` 配下にテストを、`examples/` 配下に例を追加してください。

事前の議論なしに `Source` や `Destination` プロトコルのシグネチャを変更**しないでください** — これらは将来の Rust 書き換えのために設計された安定したインターフェースです。

## 行動規範

親切で建設的であること。私たちは [Contributor Covenant](https://www.contributor-covenant.org/) に従います。

## AI スキルの更新

drt はプラグインマーケットプレイス（`skills/drt/`）を介して Claude Code スキルを提供しています。スキルの内容を更新しても、プラグインのバージョンを上げない限り、ユーザーは更新を受け取りません。

**ルール: `SKILL.md` が変更された場合は、必ず以下の3箇所でバージョンを上げてください：**

```bash
# 1. skills/drt/.claude-plugin/plugin.json
# 2. .claude-plugin/marketplace.json  (プラグインエントリのバージョン)
# 3. .claude-plugin/plugin.json       (リポジトリレベルのバージョン)
```

バージョンは `pyproject.toml` と同期してください（例: `0.4.0` をリリースする場合、すべてのプラグインバージョンを `0.4.0` に設定）。

**新しいスキル** を追加する場合は、必要に応じて `skills/drt/.claude-plugin/plugin.json` にエントリを追加し、`README.md` と `docs/llm/CONTEXT.md` にドキュメントを追加してください。
