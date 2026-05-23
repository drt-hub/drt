"""Tests for destination_lookup — FK resolution via destination DB queries."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import (
    LookupConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SyncConfig,
)
from drt.destinations.lookup import (
    apply_lookups,
    build_lookup_map,
    detect_ambiguous_lookup_ordering,
)

# ---------------------------------------------------------------------------
# LookupConfig validation
# ---------------------------------------------------------------------------


class TestLookupConfigValidation:
    def test_valid_single_match(self) -> None:
        lk = LookupConfig(
            table="users",
            match={"user_id": "user_id"},
            select="id",
        )
        assert lk.table == "users"
        assert lk.on_miss == "skip"

    def test_valid_composite_match(self) -> None:
        lk = LookupConfig(
            table="users",
            match={"company_id": "company_id", "user_id": "user_id"},
            select="id",
        )
        assert len(lk.match) == 2

    def test_valid_on_miss_options(self) -> None:
        for mode in ("skip", "fail", "null"):
            lk = LookupConfig(
                table="t",
                match={"a": "b"},
                select="id",
                on_miss=mode,  # type: ignore[arg-type]
            )
            assert lk.on_miss == mode

    def test_empty_match_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one mapping"):
            LookupConfig(table="t", match={}, select="id")


class TestLookupConfigCheckOnly:
    def test_check_only_with_select_omitted_valid(self) -> None:
        lk = LookupConfig(
            table="users",
            match={"user_id": "user_id"},
            check_only=True,
        )
        assert lk.check_only is True
        assert lk.select is None

    def test_check_only_default_is_false(self) -> None:
        lk = LookupConfig(
            table="users",
            match={"user_id": "user_id"},
            select="id",
        )
        assert lk.check_only is False

    def test_check_only_with_select_raises(self) -> None:
        with pytest.raises(ValueError, match="select"):
            LookupConfig(
                table="users",
                match={"user_id": "user_id"},
                select="id",
                check_only=True,
            )

    def test_no_check_only_without_select_raises(self) -> None:
        with pytest.raises(ValueError, match="select"):
            LookupConfig(
                table="users",
                match={"user_id": "user_id"},
            )

    def test_check_only_with_on_miss_null_raises(self) -> None:
        """on_miss=null is meaningless without a target column to NULL —
        fail at config-load instead of silently coercing to skip at runtime."""
        with pytest.raises(ValueError, match="on_miss='null' is invalid"):
            LookupConfig(
                table="users",
                match={"id": "user_id"},
                check_only=True,
                on_miss="null",
            )

    def test_check_only_with_on_miss_skip_or_fail_valid(self) -> None:
        for mode in ("skip", "fail"):
            lk = LookupConfig(
                table="users",
                match={"id": "user_id"},
                check_only=True,
                on_miss=mode,  # type: ignore[arg-type]
            )
            assert lk.on_miss == mode


# ---------------------------------------------------------------------------
# SyncConfig parsing with lookups
# ---------------------------------------------------------------------------


class TestSyncConfigWithLookups:
    def test_parse_mysql_with_lookups(self) -> None:
        config = SyncConfig.model_validate(
            {
                "name": "child_sync",
                "model": "SELECT * FROM source_table",
                "destination": {
                    "type": "mysql",
                    "host": "localhost",
                    "dbname": "testdb",
                    "table": "child_table",
                    "upsert_key": ["parent_id", "code"],
                    "lookups": {
                        "parent_id": {
                            "table": "parent_table",
                            "match": {"ext_id": "ext_id"},
                            "select": "id",
                            "on_miss": "skip",
                        },
                    },
                },
            }
        )
        assert isinstance(config.destination, MySQLDestinationConfig)
        assert config.destination.lookups is not None
        assert "parent_id" in config.destination.lookups
        lk = config.destination.lookups["parent_id"]
        assert lk.table == "parent_table"
        assert lk.select == "id"

    def test_parse_postgres_with_lookups(self) -> None:
        config = SyncConfig.model_validate(
            {
                "name": "child_sync",
                "model": "SELECT * FROM source_table",
                "destination": {
                    "type": "postgres",
                    "host": "localhost",
                    "dbname": "testdb",
                    "table": "child_table",
                    "upsert_key": ["parent_id"],
                    "lookups": {
                        "parent_id": {
                            "table": "parent_table",
                            "match": {"user_id": "user_id"},
                            "select": "id",
                        },
                    },
                },
            }
        )
        assert isinstance(config.destination, PostgresDestinationConfig)
        assert config.destination.lookups is not None

    def test_parse_without_lookups_backward_compat(self) -> None:
        config = SyncConfig.model_validate(
            {
                "name": "basic_sync",
                "model": "SELECT * FROM t",
                "destination": {
                    "type": "mysql",
                    "host": "localhost",
                    "dbname": "testdb",
                    "table": "t",
                    "upsert_key": ["id"],
                },
            }
        )
        assert isinstance(config.destination, MySQLDestinationConfig)
        assert config.destination.lookups is None

    def test_parse_multiple_lookups(self) -> None:
        config = SyncConfig.model_validate(
            {
                "name": "multi_lookup",
                "model": "SELECT * FROM t",
                "destination": {
                    "type": "postgres",
                    "host": "localhost",
                    "dbname": "testdb",
                    "table": "orders",
                    "upsert_key": ["order_id"],
                    "lookups": {
                        "customer_id": {
                            "table": "customers",
                            "match": {"email": "customer_email"},
                            "select": "id",
                        },
                        "product_id": {
                            "table": "products",
                            "match": {"sku": "product_sku"},
                            "select": "id",
                            "on_miss": "null",
                        },
                    },
                },
            }
        )
        assert isinstance(config.destination, PostgresDestinationConfig)
        assert config.destination.lookups is not None
        assert len(config.destination.lookups) == 2


# ---------------------------------------------------------------------------
# build_lookup_map
# ---------------------------------------------------------------------------


class TestBuildLookupMap:
    @patch("drt.destinations.lookup.fetch_rows")
    def test_builds_mapping_from_db_rows(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = [
            {"user_id": "u1", "id": 10},
            {"user_id": "u2", "id": 20},
            {"user_id": "u3", "id": 30},
        ]
        config = MySQLDestinationConfig(
            type="mysql",
            host="h",
            dbname="d",
            table="t",
            upsert_key=["id"],
        )
        lk = LookupConfig(
            table="parent",
            match={"user_id": "user_id"},
            select="id",
        )

        result = build_lookup_map(config, lk)

        assert result == {("u1",): 10, ("u2",): 20, ("u3",): 30}
        mock_fetch.assert_called_once()
        call_args = mock_fetch.call_args
        assert "parent" in call_args[0][1]  # query contains table name

    @patch("drt.destinations.lookup.fetch_rows")
    def test_composite_key_mapping(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = [
            {"company_id": "c1", "user_id": "u1", "id": 100},
        ]
        config = PostgresDestinationConfig(
            type="postgres",
            host="h",
            dbname="d",
            table="t",
            upsert_key=["id"],
        )
        lk = LookupConfig(
            table="parent",
            match={"company_id": "company_id", "user_id": "user_id"},
            select="id",
        )

        result = build_lookup_map(config, lk)

        assert result == {("c1", "u1"): 100}

    @patch("drt.destinations.lookup.fetch_rows")
    def test_empty_table_returns_empty_map(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = []
        config = MySQLDestinationConfig(
            type="mysql",
            host="h",
            dbname="d",
            table="t",
            upsert_key=["id"],
        )
        lk = LookupConfig(
            table="empty_table",
            match={"k": "k"},
            select="id",
        )

        result = build_lookup_map(config, lk)

        assert result == {}


class TestBuildLookupMapCheckOnly:
    @patch("drt.destinations.lookup.fetch_rows")
    def test_query_omits_select_column(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = [{"user_id": "u1"}, {"user_id": "u2"}]
        config = MySQLDestinationConfig(
            type="mysql", host="h", dbname="d", table="t", upsert_key=["id"]
        )
        lk = LookupConfig(
            table="users", match={"user_id": "user_id"}, check_only=True
        )

        build_lookup_map(config, lk)

        query = mock_fetch.call_args[0][1]
        assert "SELECT user_id FROM users" in query
        assert "id" not in query.replace("user_id", "")  # no leftover select col

    @patch("drt.destinations.lookup.fetch_rows")
    def test_returns_existence_map(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = [{"user_id": "u1"}, {"user_id": "u2"}]
        config = MySQLDestinationConfig(
            type="mysql", host="h", dbname="d", table="t", upsert_key=["id"]
        )
        lk = LookupConfig(
            table="users", match={"user_id": "user_id"}, check_only=True
        )

        result = build_lookup_map(config, lk)

        assert result == {("u1",): None, ("u2",): None}

    @patch("drt.destinations.lookup.fetch_rows")
    def test_composite_key_existence_map(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = [
            {"company_id": "c1", "user_id": "u1"},
            {"company_id": "c2", "user_id": "u2"},
        ]
        config = MySQLDestinationConfig(
            type="mysql", host="h", dbname="d", table="t", upsert_key=["id"]
        )
        lk = LookupConfig(
            table="memberships",
            match={"company_id": "company_id", "user_id": "user_id"},
            check_only=True,
        )

        result = build_lookup_map(config, lk)

        assert result == {("c1", "u1"): None, ("c2", "u2"): None}


# ---------------------------------------------------------------------------
# apply_lookups
# ---------------------------------------------------------------------------


class TestApplyLookups:
    def _make_maps(
        self,
        on_miss: str = "skip",
        mapping: dict[tuple, int] | None = None,
    ) -> dict[str, tuple[LookupConfig, dict]]:
        lk = LookupConfig(
            table="parent",
            match={"user_id": "user_id"},
            select="id",
            on_miss=on_miss,  # type: ignore[arg-type]
        )
        if mapping is None:
            mapping = {("u1",): 10, ("u2",): 20}
        return {"parent_id": (lk, mapping)}

    def test_all_match(self) -> None:
        records = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
        ]
        enriched, errors = apply_lookups(records, self._make_maps(), "fail")

        assert len(enriched) == 2
        assert enriched[0]["parent_id"] == 10
        assert enriched[1]["parent_id"] == 20
        assert "user_id" not in enriched[0]  # match col dropped by default
        assert errors == []

    def test_on_miss_skip(self) -> None:
        records = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "unknown", "name": "Ghost"},
            {"user_id": "u2", "name": "Bob"},
        ]
        enriched, errors = apply_lookups(records, self._make_maps("skip"), "skip")

        assert len(enriched) == 2
        assert enriched[0]["parent_id"] == 10
        assert enriched[1]["parent_id"] == 20
        assert len(errors) == 1
        assert "skip" in errors[0].error_message.lower()

    def test_on_miss_fail_with_on_error_fail(self) -> None:
        records = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "unknown", "name": "Ghost"},
            {"user_id": "u2", "name": "Bob"},
        ]
        enriched, errors = apply_lookups(records, self._make_maps("fail"), "fail")

        # Should stop at the first fail
        assert len(enriched) == 1
        assert enriched[0]["parent_id"] == 10
        assert len(errors) == 1

    def test_on_miss_fail_with_on_error_skip(self) -> None:
        records = [
            {"user_id": "unknown", "name": "Ghost"},
            {"user_id": "u1", "name": "Alice"},
        ]
        enriched, errors = apply_lookups(records, self._make_maps("fail"), "skip")

        # on_error=skip means we continue past failed rows
        assert len(enriched) == 1
        assert enriched[0]["parent_id"] == 10
        assert len(errors) == 1

    def test_on_miss_null(self) -> None:
        records = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "unknown", "name": "Ghost"},
        ]
        enriched, errors = apply_lookups(records, self._make_maps("null"), "fail")

        assert len(enriched) == 2
        assert enriched[0]["parent_id"] == 10
        assert enriched[1]["parent_id"] is None
        assert errors == []

    def test_multiple_lookups(self) -> None:
        lk1 = LookupConfig(
            table="parents",
            match={"user_id": "user_id"},
            select="id",
        )
        lk2 = LookupConfig(
            table="companies",
            match={"company_code": "company_code"},
            select="id",
        )
        maps: dict[str, tuple[LookupConfig, dict]] = {
            "parent_id": (lk1, {("u1",): 10}),
            "company_id": (lk2, {("acme",): 100}),
        }
        records = [{"user_id": "u1", "company_code": "acme", "name": "A"}]

        enriched, errors = apply_lookups(records, maps, "fail")

        assert len(enriched) == 1
        assert enriched[0]["parent_id"] == 10
        assert enriched[0]["company_id"] == 100

    def test_multiple_lookups_partial_miss(self) -> None:
        lk1 = LookupConfig(
            table="parents",
            match={"user_id": "user_id"},
            select="id",
        )
        lk2 = LookupConfig(
            table="companies",
            match={"company_code": "company_code"},
            select="id",
            on_miss="skip",
        )
        maps: dict[str, tuple[LookupConfig, dict]] = {
            "parent_id": (lk1, {("u1",): 10}),
            "company_id": (lk2, {("acme",): 100}),
        }
        records = [{"user_id": "u1", "company_code": "unknown", "name": "A"}]

        enriched, errors = apply_lookups(records, maps, "skip")

        assert len(enriched) == 0  # skipped due to second lookup miss
        assert len(errors) == 1

    def test_empty_records(self) -> None:
        enriched, errors = apply_lookups([], self._make_maps(), "fail")
        assert enriched == []
        assert errors == []

    def test_composite_key_match(self) -> None:
        lk = LookupConfig(
            table="parent",
            match={"company_id": "company_id", "user_id": "user_id"},
            select="id",
        )
        maps: dict[str, tuple[LookupConfig, dict]] = {
            "parent_id": (lk, {("c1", "u1"): 10, ("c2", "u2"): 20}),
        }
        records = [
            {"company_id": "c1", "user_id": "u1", "name": "A"},
            {"company_id": "c2", "user_id": "u2", "name": "B"},
        ]

        enriched, errors = apply_lookups(records, maps, "fail")

        assert len(enriched) == 2
        assert enriched[0]["parent_id"] == 10
        assert enriched[1]["parent_id"] == 20

    def test_row_error_contains_preview(self) -> None:
        records = [{"user_id": "missing", "data": "x" * 300}]
        enriched, errors = apply_lookups(records, self._make_maps("skip"), "skip")

        assert len(errors) == 1
        assert len(errors[0].record_preview) <= 200

    def test_drop_match_columns_default(self) -> None:
        """Match columns are dropped by default after lookup resolution."""
        records = [{"user_id": "u1", "name": "Alice"}]
        enriched, errors = apply_lookups(records, self._make_maps(), "fail")

        assert len(enriched) == 1
        assert enriched[0]["parent_id"] == 10
        assert "user_id" not in enriched[0]  # dropped
        assert enriched[0]["name"] == "Alice"  # non-match col preserved

    def test_drop_match_columns_false(self) -> None:
        """Match columns are kept when drop_match_columns=False."""
        lk = LookupConfig(
            table="parent",
            match={"user_id": "user_id"},
            select="id",
            drop_match_columns=False,
        )
        maps: dict[str, tuple[LookupConfig, dict]] = {
            "parent_id": (lk, {("u1",): 10}),
        }
        records = [{"user_id": "u1", "name": "Alice"}]
        enriched, errors = apply_lookups(records, maps, "fail")

        assert len(enriched) == 1
        assert enriched[0]["parent_id"] == 10
        assert enriched[0]["user_id"] == "u1"  # kept

    def test_drop_match_columns_preserves_target_col(self) -> None:
        """Don't drop a source col if it's also a target col of another lookup."""
        lk1 = LookupConfig(
            table="parents",
            match={"user_id": "user_id"},
            select="id",
        )
        lk2 = LookupConfig(
            table="teams",
            match={"team_code": "user_id"},
            select="team_id",
        )
        maps: dict[str, tuple[LookupConfig, dict]] = {
            "parent_id": (lk1, {("u1",): 10}),
            "user_id": (lk2, {("u1",): 99}),  # user_id is also a target
        }
        records = [{"user_id": "u1", "name": "Alice"}]
        enriched, errors = apply_lookups(records, maps, "fail")

        assert len(enriched) == 1
        assert enriched[0]["parent_id"] == 10
        assert enriched[0]["user_id"] == 99  # overwritten by lk2, not dropped

    def test_drop_match_columns_multiple_lookups(self) -> None:
        """Each lookup's match columns are dropped independently."""
        lk1 = LookupConfig(
            table="parents",
            match={"user_id": "user_id"},
            select="id",
        )
        lk2 = LookupConfig(
            table="companies",
            match={"company_code": "company_code"},
            select="id",
            drop_match_columns=False,
        )
        maps: dict[str, tuple[LookupConfig, dict]] = {
            "parent_id": (lk1, {("u1",): 10}),
            "company_id": (lk2, {("acme",): 100}),
        }
        records = [{"user_id": "u1", "company_code": "acme", "name": "A"}]
        enriched, errors = apply_lookups(records, maps, "fail")

        assert len(enriched) == 1
        assert "user_id" not in enriched[0]  # dropped (lk1 default)
        assert enriched[0]["company_code"] == "acme"  # kept (lk2 opt-out)


# ---------------------------------------------------------------------------
# apply_lookups — check_only mode (existence-only filtering)
# ---------------------------------------------------------------------------


class TestApplyLookupsCheckOnly:
    def _check_only_maps(
        self,
        on_miss: str = "skip",
        existing_keys: set[tuple] | None = None,
        target_name: str = "user_exists",
    ) -> dict[str, tuple[LookupConfig, dict]]:
        lk = LookupConfig(
            table="users",
            match={"id": "user_id"},
            check_only=True,
            on_miss=on_miss,  # type: ignore[arg-type]
        )
        if existing_keys is None:
            existing_keys = {("u1",), ("u2",)}
        mapping: dict[tuple, None] = dict.fromkeys(existing_keys)
        return {target_name: (lk, mapping)}

    def test_existence_hit_keeps_row_without_writing_target(self) -> None:
        records = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
        ]
        enriched, errors = apply_lookups(records, self._check_only_maps(), "fail")

        assert len(enriched) == 2
        # Source FK column preserved (purpose: still go to destination)
        assert enriched[0]["user_id"] == "u1"
        assert enriched[1]["user_id"] == "u2"
        # Target name MUST NOT be written into the row — it's an arbitrary check label
        assert "user_exists" not in enriched[0]
        assert "user_exists" not in enriched[1]
        assert errors == []

    def test_miss_with_on_miss_skip_drops_row(self) -> None:
        records = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "ghost", "name": "Ghost"},
            {"user_id": "u2", "name": "Bob"},
        ]
        enriched, errors = apply_lookups(records, self._check_only_maps("skip"), "skip")

        assert len(enriched) == 2
        assert {r["user_id"] for r in enriched} == {"u1", "u2"}
        assert len(errors) == 1
        assert "ghost" in errors[0].record_preview

    def test_miss_with_on_miss_fail_stops_sync(self) -> None:
        records = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "ghost", "name": "Ghost"},
            {"user_id": "u2", "name": "Bob"},
        ]
        enriched, errors = apply_lookups(records, self._check_only_maps("fail"), "fail")

        assert len(enriched) == 1
        assert enriched[0]["user_id"] == "u1"
        assert len(errors) == 1

    def test_check_only_does_not_drop_match_source_column(self) -> None:
        """check_only is filter-only — source columns must NOT be dropped
        even when target_name != source_col (target is just a label)."""
        records = [{"user_id": "u1", "name": "Alice"}]
        enriched, errors = apply_lookups(records, self._check_only_maps(), "fail")

        assert len(enriched) == 1
        assert enriched[0]["user_id"] == "u1"  # FK preserved for destination INSERT
        assert enriched[0]["name"] == "Alice"
        assert "user_exists" not in enriched[0]

    def test_check_only_combined_with_regular_lookup(self) -> None:
        """check_only and value-resolving lookups coexist in same record."""
        check_lk = LookupConfig(
            table="users",
            match={"id": "user_id"},
            check_only=True,
        )
        resolve_lk = LookupConfig(
            table="companies",
            match={"code": "company_code"},
            select="id",
        )
        maps: dict[str, tuple[LookupConfig, dict]] = {
            "user_exists": (check_lk, {("u1",): None}),
            "company_id": (resolve_lk, {("acme",): 99}),
        }
        records = [
            {"user_id": "u1", "company_code": "acme", "name": "A"},
            {"user_id": "ghost", "company_code": "acme", "name": "B"},
        ]

        enriched, errors = apply_lookups(records, maps, "skip")

        assert len(enriched) == 1
        assert enriched[0]["user_id"] == "u1"  # check_only preserves
        assert enriched[0]["company_id"] == 99  # regular lookup resolves
        assert "company_code" not in enriched[0]  # dropped by regular lookup default
        assert "user_exists" not in enriched[0]  # check_only never writes target


class TestSyncConfigCheckOnlyParse:
    def test_parse_check_only_yaml(self) -> None:
        config = SyncConfig.model_validate(
            {
                "name": "fk_filtered_sync",
                "model": "SELECT * FROM source",
                "destination": {
                    "type": "mysql",
                    "host": "localhost",
                    "dbname": "testdb",
                    "table": "child_table",
                    "upsert_key": ["user_id"],
                    "lookups": {
                        "user_exists": {
                            "table": "users",
                            "match": {"id": "user_id"},
                            "check_only": True,
                            "on_miss": "skip",
                        },
                    },
                },
            }
        )
        assert isinstance(config.destination, MySQLDestinationConfig)
        assert config.destination.lookups is not None
        lk = config.destination.lookups["user_exists"]
        assert lk.check_only is True
        assert lk.select is None


# ---------------------------------------------------------------------------
# detect_ambiguous_lookup_ordering (#453)
# ---------------------------------------------------------------------------


class TestDetectAmbiguousLookupOrdering:
    """Surface cases where YAML key order can flip row fate (#453)."""

    def test_empty_lookups_returns_no_warnings(self) -> None:
        assert detect_ambiguous_lookup_ordering({}) == []

    def test_single_lookup_returns_no_warnings(self) -> None:
        lookups = {
            "interviewer_profile_id": LookupConfig(
                table="interviewer_profiles",
                match={"user_id": "user_id"},
                select="id",
                on_miss="fail",
            ),
        }
        assert detect_ambiguous_lookup_ordering(lookups) == []

    def test_disjoint_source_columns_returns_no_warnings(self) -> None:
        # Two lookups, but no shared source column → order can't matter
        lookups = {
            "profile_id": LookupConfig(
                table="profiles",
                match={"user_id": "user_id"},
                select="id",
                on_miss="fail",
            ),
            "team_id": LookupConfig(
                table="teams",
                match={"slug": "team_slug"},
                select="id",
                on_miss="skip",
            ),
        }
        assert detect_ambiguous_lookup_ordering(lookups) == []

    def test_shared_source_same_on_miss_returns_no_warnings(self) -> None:
        # Same source column, same on_miss → order doesn't change fate
        lookups = {
            "a": LookupConfig(
                table="a", match={"user_id": "user_id"}, select="id", on_miss="skip"
            ),
            "b": LookupConfig(
                table="b", match={"user_id": "user_id"}, select="id", on_miss="skip"
            ),
        }
        assert detect_ambiguous_lookup_ordering(lookups) == []

    def test_shared_source_same_on_miss_different_check_only_returns_no_warnings(self) -> None:
        # Row fate on miss is determined by on_miss only. check_only only
        # affects the HIT path, so two skip-policies (one check_only, one
        # value-resolving) produce identical row outcomes — must NOT warn.
        lookups = {
            "user_exists": LookupConfig(
                table="users",
                match={"id": "user_id"},
                check_only=True,
                on_miss="skip",
            ),
            "profile_id": LookupConfig(
                table="profiles",
                match={"user_id": "user_id"},
                select="id",
                on_miss="skip",
            ),
        }
        assert detect_ambiguous_lookup_ordering(lookups) == []

    def test_shared_source_different_on_miss_returns_warning(self) -> None:
        # The exact scenario from #453
        lookups = {
            "interviewer_profile_id": LookupConfig(
                table="interviewer_profiles",
                match={"user_id": "user_id"},
                select="id",
                on_miss="fail",
            ),
            "user_exists": LookupConfig(
                table="users",
                match={"id": "user_id"},
                check_only=True,
                on_miss="skip",
            ),
        }
        warnings = detect_ambiguous_lookup_ordering(lookups)
        assert len(warnings) == 1
        assert "user_id" in warnings[0]
        assert "interviewer_profile_id" in warnings[0]
        assert "user_exists" in warnings[0]
        assert "#453" in warnings[0]

    def test_warning_per_ambiguous_source_column(self) -> None:
        # Two different source columns each ambiguous → two warnings
        lookups = {
            "a": LookupConfig(
                table="a", match={"id": "user_id"}, check_only=True, on_miss="skip"
            ),
            "b": LookupConfig(
                table="b", match={"user_id": "user_id"}, select="id", on_miss="fail"
            ),
            "c": LookupConfig(
                table="c", match={"id": "team_id"}, check_only=True, on_miss="skip"
            ),
            "d": LookupConfig(
                table="d", match={"team_id": "team_id"}, select="id", on_miss="fail"
            ),
        }
        warnings = detect_ambiguous_lookup_ordering(lookups)
        assert len(warnings) == 2
