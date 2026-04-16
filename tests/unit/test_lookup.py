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
from drt.destinations.lookup import apply_lookups, build_lookup_map

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
