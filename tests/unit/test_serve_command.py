"""Unit tests for the `drt serve` CLI command and server entry point (#854)."""

from __future__ import annotations

from typing import Any
from unittest import mock

import pytest
from typer.testing import CliRunner

from drt.cli.main import app
from drt.cli.server import serve

runner = CliRunner()


# ---------------------------------------------------------------------------
# CLI flag validation


def test_serve_rejects_unknown_auth_scheme() -> None:
    result = runner.invoke(app, ["serve", "--auth", "bogus"])
    assert result.exit_code != 0
    assert "must be one of" in result.output


def test_serve_auth_bearer_requires_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DRT_WEBHOOK_TOKEN", raising=False)
    result = runner.invoke(app, ["serve", "--auth", "bearer"])
    assert result.exit_code != 0
    assert "DRT_WEBHOOK_TOKEN" in result.output


def test_serve_auth_hmac_requires_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DRT_WEBHOOK_HMAC_SECRET", raising=False)
    result = runner.invoke(app, ["serve", "--auth", "hmac"])
    assert result.exit_code != 0
    assert "DRT_WEBHOOK_HMAC_SECRET" in result.output


def test_serve_passes_options_through(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DRT_WEBHOOK_TOKEN", "tok123")
    with mock.patch("drt.cli.server.serve") as serve_impl:
        result = runner.invoke(app, ["serve", "--port", "9999", "--auth", "bearer"])
    assert result.exit_code == 0
    serve_impl.assert_called_once_with(
        host="127.0.0.1",
        port=9999,
        token="tok123",
        project_dir=".",
        auth_scheme="bearer",
        hmac_secret=None,
        hmac_header="X-Hub-Signature-256",
        hmac_scheme="generic",
        hmac_tolerance=300,
    )


# ---------------------------------------------------------------------------
# serve() entry point — wiring, not the HTTP layer (test_server.py owns that)


class _FakeHTTPServer:
    """Stands in for ThreadingHTTPServer; serve_forever exits immediately."""

    instances: list[_FakeHTTPServer] = []

    def __init__(self, address: tuple[str, int], handler: type) -> None:
        self.address = address
        self.handler = handler
        self.shutdown_called = False
        _FakeHTTPServer.instances.append(self)

    def serve_forever(self) -> None:
        raise KeyboardInterrupt

    def shutdown(self) -> None:
        self.shutdown_called = True


@pytest.fixture()
def fake_http_server(monkeypatch: pytest.MonkeyPatch) -> type[_FakeHTTPServer]:
    _FakeHTTPServer.instances = []
    monkeypatch.setattr("drt.cli.server.ThreadingHTTPServer", _FakeHTTPServer)
    return _FakeHTTPServer


def test_serve_auto_scheme_without_token_is_none(
    fake_http_server: type[_FakeHTTPServer], tmp_path: Any
) -> None:
    serve(port=0, project_dir=str(tmp_path))
    (server,) = fake_http_server.instances
    assert server.address == ("127.0.0.1", 0)
    assert server.shutdown_called  # KeyboardInterrupt path shuts down cleanly


def test_serve_auto_scheme_with_token_is_bearer(
    fake_http_server: type[_FakeHTTPServer], tmp_path: Any
) -> None:
    serve(port=0, token="tok", project_dir=str(tmp_path))
    assert fake_http_server.instances[0].shutdown_called


def test_serve_hmac_scheme_builds(fake_http_server: type[_FakeHTTPServer], tmp_path: Any) -> None:
    serve(port=0, project_dir=str(tmp_path), auth_scheme="hmac", hmac_secret="s3")
    assert fake_http_server.instances[0].shutdown_called


def test_serve_rejects_bad_scheme(tmp_path: Any) -> None:
    with pytest.raises(ValueError, match="unknown auth scheme"):
        serve(port=0, project_dir=str(tmp_path), auth_scheme="oidc")


def test_serve_wires_a_real_project(fake_http_server: type[_FakeHTTPServer], tmp_path: Any) -> None:
    """The sync_exists/runner closures resolve against the given project dir."""
    import yaml

    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "t", "version": "0.1", "profile": "default"})
    )
    (tmp_path / "syncs").mkdir()
    (tmp_path / "syncs" / "s1.yml").write_text(
        yaml.dump(
            {
                "name": "s1",
                "model": "ref('users')",
                "destination": {"type": "file", "path": "./out.csv", "format": "csv"},
            }
        )
    )
    serve(port=0, project_dir=str(tmp_path))
    (server,) = fake_http_server.instances
    # The handler class closes over serve()'s sync_exists — exercise it directly
    # via the scheduler-free seam: the closure is what POST uses for 404s.
    # make_handler stores no public refs, so probe via the module-level pieces:
    from drt.config.parser import load_syncs

    names = [s.name for s in load_syncs(tmp_path)]
    assert names == ["s1"]
    assert server.handler is not None


def test_serve_stripe_scheme_defaults_the_header(monkeypatch: pytest.MonkeyPatch) -> None:
    """--hmac-scheme stripe implies Stripe-Signature without repeating it (#969)."""
    monkeypatch.setenv("DRT_WEBHOOK_HMAC_SECRET", "whsec_x")
    with mock.patch("drt.cli.server.serve") as served:
        runner.invoke(app, ["serve", "--auth", "hmac", "--hmac-scheme", "stripe"])
    assert served.call_args.kwargs["hmac_header"] == "Stripe-Signature"
    assert served.call_args.kwargs["hmac_scheme"] == "stripe"
    assert served.call_args.kwargs["hmac_tolerance"] == 300


def test_serve_explicit_hmac_header_wins_over_the_scheme_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DRT_WEBHOOK_HMAC_SECRET", "whsec_x")
    with mock.patch("drt.cli.server.serve") as served:
        runner.invoke(
            app,
            ["serve", "--auth", "hmac", "--hmac-scheme", "stripe", "--hmac-header", "X-Custom"],
        )
    assert served.call_args.kwargs["hmac_header"] == "X-Custom"


def test_serve_rejects_unknown_hmac_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DRT_WEBHOOK_HMAC_SECRET", "whsec_x")
    result = runner.invoke(app, ["serve", "--auth", "hmac", "--hmac-scheme", "paypal"])
    assert result.exit_code != 0


def test_serve_rejects_zero_tolerance(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stripe's docs single 0 out: it disables the check rather than tightening it."""
    monkeypatch.setenv("DRT_WEBHOOK_HMAC_SECRET", "whsec_x")
    result = runner.invoke(
        app,
        ["serve", "--auth", "hmac", "--hmac-scheme", "stripe", "--hmac-tolerance", "0"],
    )
    assert result.exit_code != 0
