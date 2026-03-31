# drtへの貢献

ご協力いただきありがとうございます！

## テストの実行

```bash
make test       # すべてのテストを実行 (pytest)
make lint       # ruff + mypy
make fmt        # 自動フォーマット (ruff format + fix)
```

コマンドを直接実行することもできます:

```bash
uv run pytest
uv run ruff check drt tests
uv run mypy drt
```


## ブランチ命名規則

| タイプ | 説明 |
| --- | --- |
| `feat` | 新機能 |
| `fix` | バグ修正 |
| `docs` | ドキュメントのみの変更 |
| `style`| コードの意味に影響を与えない変更（空白、フォーマット、セミコロンの欠落など） |
| `refactor` | バグ修正も機能追加も行わないコード変更 |
| `perf` | パフォーマンスを向上させるコード変更 |
| `test` | 不足しているテストの追加または既存のテストの修正 |
| `build`| ビルドシステムまたは外部依存関係に影響を与える変更（例：gulp、broccoli、npm） |
| `ci` | CI構成ファイルとスクリプトへの変更（例：Travis、Circle、BrowserStack、SauceLabs） |
| `chore`| `src`または`test`ファイルを変更しないその他の変更 |
| `revert` | 以前のコミットを取り消す |

## ブランチ戦略

drtは **GitHub Flow** を使用します — 全ての開発はフィーチャーブランチで行われ、直接 `main` にマージされます。

- `main` は常にリリース可能な状態です
- `develop` や `release` ブランチはありません
- リリースはタグ（`v0.2.0`、`v0.3.0`など）でマークされます

## 変更の送信

1. リポジトリをフォークします
2. 上記の命名規則に従ってブランチを作成します： `git checkout -b feat/your-feature`
3. テストを伴う変更を加えます
4. `make lint` と `make test` を実行してすべてがパスすることを確認します
5. プルリクエストを開き、PRテンプレートに記入します

> **マージ戦略:** すべてのPRは **Squash & merge** でマージされます。ブランチのコミットは `main` 上の単一のコミットにスカッシュされるため、個々のWIPコミットをクリーンアップする必要はありません。

## プルリクエストチェックリスト

- [ ] テストがパスする (`make test`)
- [ ] リンターがパスする (`make lint`)
- [ ] `CHANGELOG.md` が更新されている（ユーザー向けの変更の場合）
- [ ] 新しいコネクタには `tests/` にテストが含まれ、`examples/` に例が含まれている

## コミットスタイル

[Conventional Commits](https://www.conventionalcommits.org/) を使用してください：

```
feat: Snowflakeソースを追加
fix: REST API宛先での空のバッチを処理
docs: クイックスタートの例を更新
chore: 依存関係を更新
```

## コネクタの追加

プロトコルインターフェースについては `drt/sources/base.py` と `drt/destinations/base.py` を参照してください。
プロトコルを実装し、`tests/` にテストを追加し、`examples/` に例を追加してください。

事前の議論なしに `Source` または `Destination` プロトコルの署名を変更しないでください — これらは将来のRust書き換えのために設計された安定したインターフェースです。

## 行動規範

親切で建設的であること。私たちは[貢献者行動規範](https://www.contributor-covenant.org/)に従います。

## AIスキルの更新

drtはプラグインマーケットプレイス（`skills/drt/`）を介してClaude Codeスキルを提供します。スキルコンテンツを更新しても、プラグインのバージョンが上がらない限り、ユーザーは更新を受け取りません。

**ルール： `SKILL.md` が変更された場合は、常に3つの場所でバージョンを上げてください：**

```bash
# 1. skills/drt/.claude-plugin/plugin.json
# 2. .claude-plugin/marketplace.json  (プラグインエントリのバージョン)
# 3. .claude-plugin/plugin.json       (リポジトリレベルのバージョン)
```

バージョンは `pyproject.toml` と同期させてください（例：`0.4.0` をリリースする場合、すべてのプラグインバージョンを `0.4.0` に設定します）。

**新しいスキル**を追加する場合は、必要に応じて `skills/drt/.claude-plugin/plugin.json` にエントリを追加し、`README.md` と `docs/llm/CONTEXT.md` にそれを文書化してください。
