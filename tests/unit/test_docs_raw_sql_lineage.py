"""Postgres raw-SQL source lineage for the docs manifest (#1045)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from drt.config.parser import load_syncs_safe
from drt.docs.builder import (
    _postgres_relation_name,
    _postgres_source_relations,
    _raw_model_sql,
    build_manifest,
)
from drt.docs.dag import render_dag_svg
from drt.docs.html import render_html
from drt.docs.manifest import Manifest, Sync
from drt.docs.mermaid import render_mermaid


def _write_project(
    project_dir: Path,
    source_type: str = "postgres",
    profile: str = "warehouse",
) -> None:
    (project_dir / "drt_project.yml").write_text(
        yaml.safe_dump(
            {
                "name": "raw-lineage",
                "version": "0.1",
                "profile": profile,
                "source": {"type": source_type},
            }
        )
    )


def _write_sync(project_dir: Path, model: str) -> None:
    sync_dir = project_dir / "syncs"
    sync_dir.mkdir(exist_ok=True)
    (sync_dir / "orders.yml").write_text(
        yaml.safe_dump(
            {
                "name": "orders_to_warehouse",
                "model": model,
                "destination": {
                    "type": "postgres",
                    "host": "localhost",
                    "dbname": "warehouse",
                    "table": "analytics.orders",
                    "upsert_key": ["id"],
                },
                "sync": {"mode": "upsert"},
            }
        )
    )


def test_postgres_raw_sql_discovers_physical_relations_and_renders_them(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(
        tmp_path,
        """
        WITH recent_orders AS (
            SELECT id, customer_id FROM public.orders
        ),
        enriched AS (
            SELECT o.id, u.email
            FROM recent_orders AS o
            JOIN "CRM"."Users" AS u ON u.id = o.customer_id
        )
        SELECT enriched.id FROM enriched
        JOIN audit.events AS e ON e.order_id = enriched.id
        """,
    )

    manifest = build_manifest(tmp_path)
    sync = manifest.syncs[0]

    assert sync.sources == ('"CRM"."Users"', "audit.events", "public.orders")
    assert sync.source == '"CRM"."Users"'
    assert [(source.name, source.type) for source in manifest.sources] == [
        ('"CRM"."Users"', "postgres"),
        ("audit.events", "postgres"),
        ("public.orders", "postgres"),
    ]
    assert {(edge.from_, edge.to) for edge in manifest.edges if edge.kind == "source_to_sync"} == {
        ('"CRM"."Users"', "orders_to_warehouse"),
        ("audit.events", "orders_to_warehouse"),
        ("public.orders", "orders_to_warehouse"),
    }

    rendered = render_mermaid(manifest)
    assert "&quot;CRM&quot;.&quot;Users&quot;" in rendered
    assert "public.orders" in rendered
    assert render_dag_svg(manifest).count('marker-end="url(#dag-arr)"') == 4

    output = tmp_path / "docs"
    render_html(manifest, output)
    sync_page = (output / "sync" / "orders-to-warehouse.html").read_text(encoding="utf-8")
    assert "public.orders" in sync_page
    assert "audit.events" in sync_page
    assert (output / "source" / "public-orders.html").exists()
    assert "orders_to_warehouse" in (output / "source" / "public-orders.html").read_text(
        encoding="utf-8"
    )


def test_local_ref_override_is_parsed_as_executable_raw_postgres_sql(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path, "ref('audience')")
    model_dir = tmp_path / "syncs" / "models"
    model_dir.mkdir()
    (model_dir / "audience.sql").write_text(
        "SELECT * FROM public.customers JOIN marketing.segments USING (segment_id)"
    )

    sync = build_manifest(tmp_path).syncs[0]

    assert sync.sources == ("marketing.segments", "public.customers")


def test_raw_sql_lineage_falls_back_when_the_local_ref_cannot_be_read(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path, "ref('audience')")
    sync = load_syncs_safe(tmp_path).syncs[0]

    # An unresolved dbt ref is not guessed from the profile or dbt state.
    assert _raw_model_sql(sync, tmp_path) is None

    model_dir = tmp_path / "syncs" / "models"
    model_dir.mkdir()
    (model_dir / "audience.sql").write_text("SELECT * FROM public.customers")
    with patch.object(Path, "read_text", side_effect=OSError("file disappeared")):
        assert _raw_model_sql(sync, tmp_path) is None

    assert _postgres_source_relations(None) == ()


def test_raw_sql_relation_names_exclude_comments_and_model_text(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path, "SELECT * FROM public.customers /* api_key=not-for-docs */")

    manifest = build_manifest(tmp_path)

    assert manifest.syncs[0].sources == ("public.customers",)
    assert "api_key" not in str(manifest.to_dict())
    assert "not-for-docs" not in str(manifest.to_dict())


@pytest.mark.parametrize(
    "model",
    [
        "SELECT * INTO archive.customers FROM public.customers",
        "WITH changed AS (DELETE FROM public.customers RETURNING *) SELECT * FROM changed",
        "DELETE FROM public.customers",
        "SELECT * FROM {{ var('source_relation') }}",
    ],
)
def test_non_read_only_or_unrendered_raw_sql_keeps_the_generic_source(
    tmp_path: Path, model: str
) -> None:
    _write_project(tmp_path)
    if "var(" in model:
        _write_sync(tmp_path, "ref('templated')")
        model_dir = tmp_path / "syncs" / "models"
        model_dir.mkdir()
        (model_dir / "templated.sql").write_text(model)
    else:
        _write_sync(tmp_path, model)

    manifest = build_manifest(tmp_path)

    assert manifest.syncs[0].source == "warehouse"
    assert manifest.syncs[0].sources == ()


def test_non_identifier_table_expressions_are_not_relations() -> None:
    from sqlglot import exp

    table = exp.Table(this=exp.Anonymous(this="table_function"))

    assert _postgres_relation_name(table, exp) is None


def test_parser_recursion_or_absence_keeps_the_generic_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sqlglot.optimizer.scope

    def _raise_recursion(*_args: object, **_kwargs: object) -> None:
        raise RecursionError("nested query too deep")

    monkeypatch.setattr(sqlglot.optimizer.scope, "traverse_scope", _raise_recursion)
    assert _postgres_source_relations("SELECT * FROM public.customers") == ()

    monkeypatch.setitem(sys.modules, "sqlglot", None)
    assert _postgres_source_relations("SELECT * FROM public.customers") == ()


def test_profile_and_relation_sources_with_the_same_name_stay_distinct(tmp_path: Path) -> None:
    _write_project(tmp_path, profile="users")
    _write_sync(tmp_path, "SELECT * FROM users")
    (tmp_path / "syncs" / "fallback.yml").write_text(
        yaml.safe_dump(
            {
                "name": "fallback_to_warehouse",
                "model": "ref('unresolved')",
                "destination": {
                    "type": "postgres",
                    "host": "localhost",
                    "dbname": "warehouse",
                    "table": "analytics.fallback",
                    "upsert_key": ["id"],
                },
                "sync": {"mode": "upsert"},
            }
        )
    )

    manifest = build_manifest(tmp_path)

    assert [(source.name, source.type) for source in manifest.sources] == [
        ("profile:users", "postgres"),
        ("users", "postgres"),
    ]
    assert {(edge.from_, edge.to) for edge in manifest.edges if edge.kind == "source_to_sync"} == {
        ("users", "orders_to_warehouse"),
        ("profile:users", "fallback_to_warehouse"),
    }


def test_unsupported_or_ambiguous_models_keep_the_generic_source(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path, "SELECT * FROM public.orders; SELECT * FROM audit.events")

    manifest = build_manifest(tmp_path)

    assert manifest.syncs[0].source == "warehouse"
    assert manifest.syncs[0].sources == ()
    assert [(source.name, source.type) for source in manifest.sources] == [
        ("warehouse", "postgres")
    ]


def test_invalid_postgres_sql_keeps_the_generic_source(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path, "SELECT * FROM (")

    manifest = build_manifest(tmp_path)

    assert manifest.syncs[0].source == "warehouse"
    assert manifest.syncs[0].sources == ()


def test_non_postgres_project_does_not_parse_raw_sql(tmp_path: Path) -> None:
    _write_project(tmp_path, source_type="bigquery")
    _write_sync(tmp_path, "SELECT * FROM public.orders JOIN audit.events ON true")

    manifest = build_manifest(tmp_path)

    assert manifest.syncs[0].source == "warehouse"
    assert manifest.syncs[0].sources == ()
    assert [(source.name, source.type) for source in manifest.sources] == [
        ("warehouse", "bigquery")
    ]


def test_v8_sources_round_trip_and_old_manifest_omits_them() -> None:
    v8 = Manifest(
        schema_version=8,
        drt_version="1.3.0",
        syncs=[
            Sync(
                name="s",
                source="public.orders",
                sources=("public.orders", "public.customers"),
                destination="d",
                mode="upsert",
            )
        ],
    )

    assert Manifest.from_dict(v8.to_dict()) == v8
    old_shape = {
        "schema_version": 7,
        "drt_version": "1.3.0",
        "syncs": [{"name": "s", "source": "warehouse", "destination": "d", "mode": "upsert"}],
    }
    assert "sources" not in Manifest.from_dict(old_shape).to_dict()["syncs"][0]


def test_v8_sources_rejects_non_string_relation_names() -> None:
    document = {
        "schema_version": 8,
        "drt_version": "1.3.0",
        "syncs": [
            {
                "name": "s",
                "source": "public.orders",
                "destination": "d",
                "mode": "upsert",
                "sources": ["public.orders", 42],
            }
        ],
    }

    with pytest.raises(ValueError, match="sources must be an array of strings"):
        Manifest.from_dict(document)
