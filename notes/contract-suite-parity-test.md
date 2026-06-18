# Memo: `registry ⊆ contract suite` パリティテスト（後でIssue化する）

> ステータス: メモ。レビュー中に発見。後で GitHub Issue にする。
> 発見元: PR #647 (Airtable) / PR #648 (Klaviyo) のレビュー。

## 問題（背景）

新規 destination を追加する PR が、共通の **contract テストスイート**
（空バッチ契約など）への登録を毎回スルーしている。

- PR #647 (Airtable) と PR #648 (Klaviyo) はどちらも:
  - registry / CLI 一覧 (`DESTINATIONS`) / `_connector_detail` / MCP inventory /
    docs / README / skill / 個別ユニットテストは更新済み
  - **しかし `tests/contracts/` と `tests/unit/test_destination_contract.py`
    には未登録**
- これは単発ミスではなく **新規 destination PR 共通のパターン**。
- 根本原因: 「登録済み destination ⊆ contract スイート」を強制する
  パリティテストが**存在しない**ため、漏れていても CI が緑のまま通る。
  contract スイートの param リストはファイルごとの手動メンテ。

CLAUDE.md / CHANGELOG にある `test_DESTINATIONS_matches_registry`
（registry ⊆ `DESTINATIONS` を保証）と**同じ思想**だが、その保証は
CLI 一覧用で contract スイートには適用されていない。

## 提案

`registry ⊆ contract suite` を保証するパリティテストを 1 本追加する。
contract スイートはファイル分割（下記）なので、その**和集合**が
registry を覆っているかを検証する。

- `tests/contracts/test_destination_api_empty_batch.py::API_DESTINATIONS`
- `tests/contracts/test_destination_empty_batch.py::HTTP_DESTINATIONS`
- `tests/contracts/test_destination_sql_empty_batch.py::SQL_DESTINATIONS`
- `tests/contracts/test_destination_file_empty_batch.py::FILE_DESTINATIONS`
- `tests/contracts/test_destination_staged_empty_batch.py::STAGED_DESTINATIONS`
- `tests/contracts/test_destination_special_empty_batch.py::SPECIAL_DESTINATIONS`

### スケッチ

```python
def test_every_registered_destination_has_a_contract_test() -> None:
    import drt.connectors.registry as registry
    # 各 param リストの和集合から「テスト対象クラス」を収集
    covered = collect_classes(API_DESTINATIONS, HTTP_DESTINATIONS, SQL_DESTINATIONS,
                              FILE_DESTINATIONS, STAGED_DESTINATIONS, SPECIAL_DESTINATIONS)
    registered = set(registry._destination_registry.values())
    missing = registered - covered
    assert not missing, (
        f"contract スイート未登録の destination: "
        f"{sorted(c.__name__ for c in missing)}. "
        "対応するファイルの param リストに追加してください。"
    )
```

## 実装メモ / 注意点

1. **既存の網羅漏れを今回まとめて埋める**:
   - airtable → `API_DESTINATIONS` + `ALL_DESTINATIONS`
   - klaviyo  → `API_DESTINATIONS` + `ALL_DESTINATIONS`
   - テスト追加で他にも漏れが出れば一緒に登録する。

2. **`ConnectionTestable` 分類の落とし穴**:
   airtable / klaviyo は `test_connection` を実装しているため、
   `@runtime_checkable` な `ConnectionTestable` を満たす。
   `tests/unit/test_destination_contract.py` の
   `NON_CONNECTION_TESTABLE_DESTINATIONS` に入れると
   `test_non_sql_destinations_do_not_implement_connection_testable` が落ちる。
   現状 `CONNECTION_TESTABLE_DESTINATIONS` は SQL 専用
   （テスト名も `test_sql_destinations_...`）。
   → **初の非SQLな `ConnectionTestable`** になるので、リスト分類の
   見直し（命名含む）が必要。単純 append で済まない。

3. **完全一致 vs 部分集合**:
   `email_smtp` / `google_sheets` / `salesforce_bulk` / `staged_upload` は
   httpx 以外のトランスポートで意図的に別スイート扱い。
   最初は `registered ⊆ covered`（covered の余剰を許し、漏れだけ検出）が
   運用しやすい。完全一致 (`==`) にすると全分類を正確に拾う必要がある。

4. param から第1要素（destination クラス）を取り出す小ヘルパーが要る
   （`pytest.param(...)` をアンラップ）。

## 効果

- 新規 destination を contract テストに入れ忘れると CI が即赤くなる。
- レビューで毎回手で気づく必要がなくなる（ドリフト再発防止）。
- registry を唯一の真実とし、テスト網羅をそこに追従させる
  （`test_DESTINATIONS_matches_registry` と同じ方針）。

## ラベル候補（Issue 化時）

`chore` / `refactor`（テスト基盤強化）。関連 PR: #647, #648。
