"""Tests for the webhook trigger server (#854 concurrency contract)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from drt import __version__
from drt.cli.server import (
    AuthConfig,
    SyncScheduler,
    _verify_hmac,
    _verify_stripe_hmac,
    make_handler,
)

_SUCCESS_RESULT = {
    "sync_name": "s",
    "status": "success",
    "rows_synced": 1,
    "rows_failed": 0,
    "duration_seconds": 0.1,
    "dry_run": False,
    "errors": [],
}


def _run_server(
    runner: Callable[[str, bool], dict[str, Any]] | None = None,
    auth: AuthConfig | None = None,
    sync_exists: Callable[[str], bool] | None = None,
) -> tuple[ThreadingHTTPServer, SyncScheduler, int]:
    """Start a server on a random port and return (server, scheduler, port)."""
    scheduler = SyncScheduler(runner or (lambda name, dry: dict(_SUCCESS_RESULT)))
    handler = make_handler(auth or AuthConfig(), scheduler, sync_exists or (lambda name: True))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, scheduler, port


def _request(
    url: str,
    method: str = "GET",
    token: str | None = None,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, Any]]:
    req = urllib.request.Request(url, data=body, method=method)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    for name, value in (headers or {}).items():
        req.add_header(name, value)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def _get(
    url: str, token: str | None = None, headers: dict[str, str] | None = None
) -> tuple[int, dict[str, Any]]:
    return _request(url, token=token, headers=headers)


def _post(url: str, token: str | None = None, **kwargs: Any) -> tuple[int, dict[str, Any]]:
    return _request(url, method="POST", token=token, **kwargs)


def _wait_for(predicate: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _poll_state(port: int, run_id: str, timeout: float = 5.0) -> dict[str, Any]:
    """Poll GET /runs/<id> until the run reaches a terminal state."""
    terminal = {"success", "partial", "failed", "error"}
    body: dict[str, Any] = {}

    def _done() -> bool:
        nonlocal body
        status, body = _get(f"http://127.0.0.1:{port}/runs/{run_id}")
        assert status == 200
        return body["state"] in terminal

    assert _wait_for(_done, timeout), f"run {run_id} never finished: {body}"
    return body


# ---------------------------------------------------------------------------
# Basics (unchanged surface)


def test_health_endpoint() -> None:
    server, _, port = _run_server()
    try:
        status, body = _get(f"http://127.0.0.1:{port}/health")
        assert status == 200
        assert body == {"status": "ok", "version": __version__}
    finally:
        server.shutdown()


def test_unknown_get_returns_404() -> None:
    server, _, port = _run_server()
    try:
        status, body = _get(f"http://127.0.0.1:{port}/unknown")
        assert status == 404
        assert "error" in body
    finally:
        server.shutdown()


def test_post_missing_sync_name_returns_400() -> None:
    server, _, port = _run_server()
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/")
        assert status == 400
        assert "sync name" in body["error"]
    finally:
        server.shutdown()


def test_post_unknown_sync_returns_404() -> None:
    server, _, port = _run_server(sync_exists=lambda name: False)
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/nonexistent")
        assert status == 404
        assert "nonexistent" in body["error"]
    finally:
        server.shutdown()


def test_post_unknown_sync_returns_404_via_real_loader(tmp_path: Path) -> None:
    """The sync_exists wiring serve() uses, against a real (empty) project."""
    import yaml

    from drt.config.parser import load_syncs

    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "test", "version": "0.1", "profile": "default"})
    )
    (tmp_path / "syncs").mkdir()

    def sync_exists(name: str) -> bool:
        return any(s.name == name for s in load_syncs(tmp_path))

    server, _, port = _run_server(sync_exists=sync_exists)
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/nonexistent")
        assert status == 404
        assert "nonexistent" in body["error"]
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# Auth


def test_post_without_auth_when_required_returns_401() -> None:
    server, _, port = _run_server(auth=AuthConfig(scheme="bearer", token="secret123"))
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/my_sync")
        assert status == 401
        assert "unauthorized" in body["error"]
    finally:
        server.shutdown()


def test_post_with_wrong_token_returns_401() -> None:
    server, _, port = _run_server(auth=AuthConfig(scheme="bearer", token="secret123"))
    try:
        status, _ = _post(f"http://127.0.0.1:{port}/sync/my_sync", token="wrong")
        assert status == 401
    finally:
        server.shutdown()


def test_post_with_correct_token_accepted() -> None:
    server, _, port = _run_server(auth=AuthConfig(scheme="bearer", token="secret123"))
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/my_sync", token="secret123")
        assert status == 202
        assert body["run_id"]
    finally:
        server.shutdown()


@pytest.mark.parametrize("encoding", ["github", "hex", "base64"])
def test_hmac_valid_signature_accepted(encoding: str) -> None:
    secret, payload = "topsecret", b'{"event": "push"}'
    digest = hmac.new(secret.encode(), payload, hashlib.sha256)
    signature = {
        "github": f"sha256={digest.hexdigest()}",
        "hex": digest.hexdigest(),
        "base64": base64.b64encode(digest.digest()).decode(),
    }[encoding]

    server, _, port = _run_server(auth=AuthConfig(scheme="hmac", hmac_secret=secret))
    try:
        status, body = _post(
            f"http://127.0.0.1:{port}/sync/my_sync",
            body=payload,
            headers={"X-Hub-Signature-256": signature},
        )
        assert status == 202
        assert body["run_id"]
    finally:
        server.shutdown()


def test_hmac_wrong_signature_rejected() -> None:
    server, _, port = _run_server(auth=AuthConfig(scheme="hmac", hmac_secret="topsecret"))
    try:
        status, _ = _post(
            f"http://127.0.0.1:{port}/sync/my_sync",
            body=b"{}",
            headers={"X-Hub-Signature-256": "sha256=" + "0" * 64},
        )
        assert status == 401
    finally:
        server.shutdown()


def test_hmac_missing_signature_rejected() -> None:
    server, _, port = _run_server(auth=AuthConfig(scheme="hmac", hmac_secret="topsecret"))
    try:
        status, _ = _post(f"http://127.0.0.1:{port}/sync/my_sync", body=b"{}")
        assert status == 401
    finally:
        server.shutdown()


def test_get_run_without_auth_when_required_returns_401() -> None:
    """GET /runs/<id> carries the SyncResult, so it is authenticated like the POST.

    The run id is a uuid4, but it rides in the URL path and lands in every
    proxy access log, which is not where a bearer credential belongs.
    """
    server, _, port = _run_server(auth=AuthConfig(scheme="bearer", token="secret123"))
    try:
        _, accepted = _post(f"http://127.0.0.1:{port}/sync/s", token="secret123")
        status, body = _get(f"http://127.0.0.1:{port}/runs/{accepted['run_id']}")
        assert status == 401
        assert "unauthorized" in body["error"]
    finally:
        server.shutdown()


def test_get_run_with_correct_token_returns_state() -> None:
    server, _, port = _run_server(auth=AuthConfig(scheme="bearer", token="secret123"))
    try:
        _, accepted = _post(f"http://127.0.0.1:{port}/sync/s", token="secret123")
        status, body = _get(f"http://127.0.0.1:{port}/runs/{accepted['run_id']}", token="secret123")
        assert status == 200
        assert body["run_id"] == accepted["run_id"]
    finally:
        server.shutdown()


def test_unauthenticated_get_does_not_leak_which_run_ids_exist() -> None:
    """A real id and an invented one answer alike without a credential."""
    server, _, port = _run_server(auth=AuthConfig(scheme="bearer", token="secret123"))
    try:
        _, accepted = _post(f"http://127.0.0.1:{port}/sync/s", token="secret123")
        real, _ = _get(f"http://127.0.0.1:{port}/runs/{accepted['run_id']}")
        invented, _ = _get(f"http://127.0.0.1:{port}/runs/deadbeef")
        assert real == invented == 401
    finally:
        server.shutdown()


def test_health_stays_open_under_auth() -> None:
    """A load-balancer probe must not need a credential."""
    server, _, port = _run_server(auth=AuthConfig(scheme="bearer", token="secret123"))
    try:
        status, body = _get(f"http://127.0.0.1:{port}/health")
        assert status == 200
        assert body["status"] == "ok"
    finally:
        server.shutdown()


def _get_sig(secret: str, path: str) -> str:
    """The signature a client computes for ``GET <path>``."""
    key = hmac.new(secret.encode(), b"drt/serve/v1/get-path", hashlib.sha256).digest()
    return "sha256=" + hmac.new(key, path.encode(), hashlib.sha256).hexdigest()


def test_hmac_get_signs_the_request_path() -> None:
    """A GET has no body, so its signature covers the path it asks for."""
    secret = "topsecret"
    payload = b'{"event": "push"}'
    post_sig = "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()

    server, _, port = _run_server(auth=AuthConfig(scheme="hmac", hmac_secret=secret))
    try:
        _, accepted = _post(
            f"http://127.0.0.1:{port}/sync/s",
            body=payload,
            headers={"X-Hub-Signature-256": post_sig},
        )
        url = f"http://127.0.0.1:{port}/runs/{accepted['run_id']}"
        assert _get(url)[0] == 401
        # The POST's signature is over its own body and must not open the GET
        assert _get(url, headers={"X-Hub-Signature-256": post_sig})[0] == 401
        sig = _get_sig(secret, f"/runs/{accepted['run_id']}")
        status, body = _get(url, headers={"X-Hub-Signature-256": sig})
        assert status == 200
        assert body["run_id"] == accepted["run_id"]
    finally:
        server.shutdown()


def test_hmac_get_signature_does_not_open_a_different_run() -> None:
    """#936: a signature issued for run A must not read run B."""
    secret = "topsecret"
    payload = b'{"event": "push"}'
    post_sig = "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()

    server, _, port = _run_server(auth=AuthConfig(scheme="hmac", hmac_secret=secret))
    try:
        base = f"http://127.0.0.1:{port}"
        _, a = _post(f"{base}/sync/s", body=payload, headers={"X-Hub-Signature-256": post_sig})
        _, b = _post(f"{base}/sync/s", body=payload, headers={"X-Hub-Signature-256": post_sig})
        assert a["run_id"] != b["run_id"]

        header = {"X-Hub-Signature-256": _get_sig(secret, f"/runs/{a['run_id']}")}
        # Opens the run it was issued for...
        assert _get(f"{base}/runs/{a['run_id']}", headers=header)[0] == 200
        # ...and nothing else.
        assert _get(f"{base}/runs/{b['run_id']}", headers=header)[0] == 401
    finally:
        server.shutdown()


def test_hmac_get_signature_cannot_be_replayed_as_a_post() -> None:
    """A captured GET signature must not authenticate a sync trigger.

    GET signs a path and POST signs a body. Under one key an attacker could
    send the GET's signed payload *as* a body and reuse the digest, turning a
    leaked read-only signature into "trigger any sync". The keys differ, so the
    digests cannot collide.
    """
    secret = "topsecret"
    payload = b'{"event": "push"}'
    post_sig = "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()

    server, _, port = _run_server(auth=AuthConfig(scheme="hmac", hmac_secret=secret))
    try:
        base = f"http://127.0.0.1:{port}"
        _, run = _post(f"{base}/sync/s", body=payload, headers={"X-Hub-Signature-256": post_sig})
        path = f"/runs/{run['run_id']}"
        stolen = _get_sig(secret, path)
        assert _get(f"{base}{path}", headers={"X-Hub-Signature-256": stolen})[0] == 200

        # Every shape of body an attacker would craft from the signed path.
        for crafted in (path.encode(), path.encode() + b"\x00", b"\x00" + path.encode()):
            status, _ = _post(
                f"{base}/sync/other",
                body=crafted,
                headers={"X-Hub-Signature-256": stolen},
            )
            assert status == 401, f"GET signature replayed as POST with body {crafted!r}"
    finally:
        server.shutdown()


def test_hmac_bodyless_post_still_signs_the_empty_body() -> None:
    """POST /sync/<name> is documented as bodyless; that contract is unchanged."""
    secret = "topsecret"
    empty_sig = "sha256=" + hmac.new(secret.encode(), b"", hashlib.sha256).hexdigest()

    server, _, port = _run_server(auth=AuthConfig(scheme="hmac", hmac_secret=secret))
    try:
        status, body = _post(
            f"http://127.0.0.1:{port}/sync/s",
            body=b"",
            headers={"X-Hub-Signature-256": empty_sig},
        )
        assert status == 202, "an existing bodyless-POST client must not start 401ing"
        assert "run_id" in body
    finally:
        server.shutdown()


def test_auth_config_rejects_misconfiguration() -> None:
    with pytest.raises(ValueError, match="requires a token"):
        AuthConfig(scheme="bearer")
    with pytest.raises(ValueError, match="requires a secret"):
        AuthConfig(scheme="hmac")
    with pytest.raises(ValueError, match="unknown auth scheme"):
        AuthConfig(scheme="oidc")


# ---------------------------------------------------------------------------
# Async contract: 202 + GET /runs/<id>


def test_trigger_returns_202_and_run_completes() -> None:
    server, _, port = _run_server()
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/s")
        assert status == 202
        assert body["coalesced"] is False
        assert body["url"] == f"/runs/{body['run_id']}"
        run = _poll_state(port, body["run_id"])
        assert run["state"] == "success"
        assert run["result"]["rows_synced"] == 1
        assert run["started_at"] is not None
        assert run["finished_at"] is not None
    finally:
        server.shutdown()


def test_get_unknown_run_returns_404() -> None:
    server, _, port = _run_server()
    try:
        status, body = _get(f"http://127.0.0.1:{port}/runs/deadbeef")
        assert status == 404
        assert "lifetime" in body["error"]
    finally:
        server.shutdown()


def test_finished_runs_evict_on_a_status_outside_the_known_set() -> None:
    """Eviction keys on finished_at, not on a whitelist of state strings.

    The state string comes from the runner's result contract; a status added
    there later must not leave runs permanently un-evictable in ``_runs``.
    """
    scheduler = SyncScheduler(lambda name, dry: {"status": "quiesced"}, max_finished=2)
    for i in range(5):
        run, _ = scheduler.trigger(f"sync_{i}", False)
        assert run.done.wait(timeout=10)
        assert run.state == "quiesced"

    # Eviction runs when the next run is created, so the cap is max_finished
    # plus the run that has not finished being accounted for yet.
    assert len(scheduler._runs) <= 3


def test_runner_exception_surfaces_as_error_state() -> None:
    def runner(name: str, dry: bool) -> dict[str, Any]:
        raise RuntimeError("boom")

    server, _, port = _run_server(runner=runner)
    try:
        _, body = _post(f"http://127.0.0.1:{port}/sync/s")
        run = _poll_state(port, body["run_id"])
        assert run["state"] == "error"
        assert "boom" in run["error"]
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# wait=true: the pre-#854 synchronous contract


def test_wait_true_returns_result_synchronously() -> None:
    server, _, port = _run_server()
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/s?wait=true")
        assert status == 200
        assert body["status"] == "success"
        # Sequential triggers keep working (nothing left locked)
        status2, _ = _post(f"http://127.0.0.1:{port}/sync/s?wait=true")
        assert status2 == 200
    finally:
        server.shutdown()


@pytest.mark.parametrize("result_status", ["partial", "failed"])
def test_wait_true_non_success_returns_207(result_status: str) -> None:
    result = dict(_SUCCESS_RESULT, status=result_status)
    server, _, port = _run_server(runner=lambda name, dry: result)
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/s?wait=true")
        assert status == 207
        assert body["status"] == result_status
    finally:
        server.shutdown()


def test_wait_true_runner_exception_returns_500() -> None:
    def runner(name: str, dry: bool) -> dict[str, Any]:
        raise RuntimeError("boom")

    server, _, port = _run_server(runner=runner)
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/s?wait=true")
        assert status == 500
        assert "boom" in body["error"]
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# The concurrency contract itself


class _BlockingRunner:
    """Runner that blocks each call until released, recording every call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []
        self.release = threading.Event()
        self._lock = threading.Lock()

    def __call__(self, name: str, dry: bool) -> dict[str, Any]:
        with self._lock:
            self.calls.append((name, dry))
        assert self.release.wait(timeout=10)
        return dict(_SUCCESS_RESULT, sync_name=name, dry_run=dry)


def test_same_sync_triggers_coalesce_to_depth_one() -> None:
    runner = _BlockingRunner()
    server, _, port = _run_server(runner=runner)
    try:
        _, first = _post(f"http://127.0.0.1:{port}/sync/s")
        assert first["coalesced"] is False

        _, second = _post(f"http://127.0.0.1:{port}/sync/s")
        assert second["coalesced"] is True
        assert second["state"] == "pending"
        assert second["run_id"] != first["run_id"]

        # A third trigger while one is pending returns the SAME pending run
        _, third = _post(f"http://127.0.0.1:{port}/sync/s")
        assert third["coalesced"] is True
        assert third["run_id"] == second["run_id"]

        runner.release.set()
        assert _poll_state(port, first["run_id"])["state"] == "success"
        assert _poll_state(port, second["run_id"])["state"] == "success"
        # Three triggers, exactly two executions — coalesced, not dropped
        assert len(runner.calls) == 2
    finally:
        server.shutdown()


def test_different_syncs_run_concurrently() -> None:
    blocker = threading.Event()

    def runner(name: str, dry: bool) -> dict[str, Any]:
        if name == "slow":
            assert blocker.wait(timeout=10)
        return dict(_SUCCESS_RESULT, sync_name=name)

    server, _, port = _run_server(runner=runner)
    try:
        _, slow = _post(f"http://127.0.0.1:{port}/sync/slow")
        _, fast = _post(f"http://127.0.0.1:{port}/sync/fast")
        assert fast["coalesced"] is False  # not queued behind the other sync

        # "fast" finishes while "slow" is still running
        assert _poll_state(port, fast["run_id"])["state"] == "success"
        _, slow_now = _get(f"http://127.0.0.1:{port}/runs/{slow['run_id']}")
        assert slow_now["state"] == "running"

        blocker.set()
        assert _poll_state(port, slow["run_id"])["state"] == "success"
    finally:
        server.shutdown()


def test_dry_run_and_real_triggers_coalesce_separately() -> None:
    runner = _BlockingRunner()
    server, _, port = _run_server(runner=runner)
    try:
        _, first = _post(f"http://127.0.0.1:{port}/sync/s")
        _, real_pending = _post(f"http://127.0.0.1:{port}/sync/s")
        _, dry_pending = _post(f"http://127.0.0.1:{port}/sync/s?dry_run=true")
        # A real trigger must never fold into a dry preview (or vice versa)
        assert dry_pending["run_id"] != real_pending["run_id"]
        assert dry_pending["coalesced"] is True

        runner.release.set()
        assert _poll_state(port, first["run_id"])["state"] == "success"
        assert _poll_state(port, real_pending["run_id"])["state"] == "success"
        assert _poll_state(port, dry_pending["run_id"])["state"] == "success"
        assert len(runner.calls) == 3
        assert (("s", True) in runner.calls) and (("s", False) in runner.calls)
    finally:
        server.shutdown()


def test_wait_true_while_busy_coalesces_and_blocks_until_done() -> None:
    runner = _BlockingRunner()
    server, _, port = _run_server(runner=runner)
    try:
        _, first = _post(f"http://127.0.0.1:{port}/sync/s")

        outcome: list[tuple[int, dict[str, Any]]] = []
        waiter = threading.Thread(
            target=lambda: outcome.append(_post(f"http://127.0.0.1:{port}/sync/s?wait=true")),
            daemon=True,
        )
        waiter.start()
        # The waiter must be blocked on the coalesced pending run, not answered
        assert not _wait_for(lambda: bool(outcome), timeout=0.3)

        runner.release.set()
        waiter.join(timeout=10)
        assert outcome, "wait=true request never returned"
        status, body = outcome[0]
        assert status == 200
        assert body["status"] == "success"
        assert _poll_state(port, first["run_id"])["state"] == "success"
    finally:
        runner.release.set()
        server.shutdown()


# ---------------------------------------------------------------------------
# Stripe timestamped signatures (#969)


def _stripe_header(secret: str, body: bytes, *, at: int, extra: str = "") -> str:
    signature = hmac.new(secret.encode(), f"{at}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={at},v1={signature}{extra}"


def test_stripe_valid_signature_accepted() -> None:
    secret, payload = "whsec_topsecret", b'{"id": "evt_1", "type": "charge.succeeded"}'
    now = int(time.time())
    server, _, port = _run_server(
        auth=AuthConfig(
            scheme="hmac",
            hmac_secret=secret,
            hmac_scheme="stripe",
            hmac_header="Stripe-Signature",
        )
    )
    try:
        status, body = _post(
            f"http://127.0.0.1:{port}/sync/my_sync",
            body=payload,
            headers={"Stripe-Signature": _stripe_header(secret, payload, at=now)},
        )
        assert status == 202
        assert body["run_id"]
    finally:
        server.shutdown()


def test_stripe_signature_covers_the_body() -> None:
    """The signature is over ``t.body``; swapping the body must fail."""
    secret = "whsec_topsecret"
    now = int(time.time())
    header = _stripe_header(secret, b'{"amount": 100}', at=now)
    server, _, port = _run_server(
        auth=AuthConfig(
            scheme="hmac", hmac_secret=secret, hmac_scheme="stripe", hmac_header="Stripe-Signature"
        )
    )
    try:
        status, _ = _post(
            f"http://127.0.0.1:{port}/sync/my_sync",
            body=b'{"amount": 999999}',
            headers={"Stripe-Signature": header},
        )
        assert status == 401
    finally:
        server.shutdown()


def test_stripe_stale_timestamp_rejected() -> None:
    """A replayed delivery outside the window is refused even though it signs."""
    secret, payload = "whsec_topsecret", b"{}"
    stale = int(time.time()) - 601
    server, _, port = _run_server(
        auth=AuthConfig(
            scheme="hmac",
            hmac_secret=secret,
            hmac_scheme="stripe",
            hmac_header="Stripe-Signature",
            hmac_tolerance=300,
        )
    )
    try:
        status, _ = _post(
            f"http://127.0.0.1:{port}/sync/my_sync",
            body=payload,
            headers={"Stripe-Signature": _stripe_header(secret, payload, at=stale)},
        )
        assert status == 401
    finally:
        server.shutdown()


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        # A fake v0 rides along with real events; it must be ignored, not fail the request.
        ("with_v0", True),
        # Rolling an endpoint secret leaves two live, and Stripe signs once per secret.
        ("rotation", True),
        # "Ignore all schemes that aren't v1" — a v0-only header is a downgrade attempt.
        ("v0_only", False),
        ("no_timestamp", False),
        ("bad_timestamp", False),
        ("duplicate_timestamp", False),
        # Malformed input is unauthenticated and attacker-chosen, so it has to
        # return False rather than raise out of the verifier and take the
        # connection down with a traceback. Both of these used to raise:
        # compare_digest refuses non-ASCII str operands (TypeError), and an
        # arbitrary-precision int overflows on the float comparison
        # (OverflowError) despite parsing cleanly.
        ("non_ascii_signature", False),
        ("oversized_timestamp", False),
    ],
)
def test_stripe_header_parsing_edge_cases(header: str, expected: bool) -> None:
    secret, payload = "whsec_topsecret", b'{"id": "evt_1"}'
    now = int(time.time())
    good = hmac.new(secret.encode(), f"{now}.".encode() + payload, hashlib.sha256).hexdigest()
    headers = {
        "with_v0": f"t={now},v1={good},v0=deadbeef",
        "rotation": f"t={now},v1={'0' * 64},v1={good}",
        "v0_only": f"t={now},v0={good}",
        "no_timestamp": f"v1={good}",
        "bad_timestamp": f"t=not-a-number,v1={good}",
        "duplicate_timestamp": f"t={now},t={now},v1={good}",
        "non_ascii_signature": f"t={now},v1=\u00e9",
        "oversized_timestamp": f"t={'9' * 400},v1={good}",
    }
    assert _verify_stripe_hmac(payload, headers[header], secret, 300) is expected


def test_stripe_replay_window_is_symmetric() -> None:
    """A far-future timestamp is as suspect as a stale one.

    Stripe's own libraries only reject stale timestamps, which leaves a
    future-dated signature valid indefinitely.
    """
    secret, payload = "whsec_topsecret", b"{}"
    base = 1_700_000_000
    header = _stripe_header(secret, payload, at=base)
    assert _verify_stripe_hmac(payload, header, secret, 300, now=base) is True
    assert _verify_stripe_hmac(payload, header, secret, 300, now=base + 301) is False
    assert _verify_stripe_hmac(payload, header, secret, 300, now=base - 301) is False


def test_stripe_scheme_does_not_accept_a_plain_body_signature() -> None:
    """The generic and Stripe schemes must not silently accept each other."""
    secret, payload = "whsec_topsecret", b"{}"
    plain = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    assert _verify_stripe_hmac(payload, f"sha256={plain}", secret, 300) is False
    assert _verify_hmac(payload, _stripe_header(secret, payload, at=int(time.time())), secret) is (
        False
    )


def test_stripe_scheme_rejects_get_regardless_of_signature() -> None:
    """`--hmac-scheme stripe` is POST-only, so a GET is 401 even if it signs.

    Stripe never sends a GET, so there is no header shape it defines for one.
    Falling through to the generic path would be worse than a flat rejection:
    a Stripe signature over the empty body carries no path, so one captured
    header would read any run id for the whole tolerance window — exactly the
    replay #998 closed for the generic scheme.
    """
    secret = "whsec_topsecret"
    now = int(time.time())
    server, _, port = _run_server(
        auth=AuthConfig(
            scheme="hmac",
            hmac_secret=secret,
            hmac_scheme="stripe",
            hmac_header="Stripe-Signature",
        )
    )
    try:
        # /health stays exempt, so a probe still works without a credential.
        assert _get(f"http://127.0.0.1:{port}/health")[0] == 200
        # A correctly signed empty body — the closest thing to a valid GET
        # signature this scheme could have — is still refused.
        status, _ = _get(
            f"http://127.0.0.1:{port}/runs/whatever",
            headers={"Stripe-Signature": _stripe_header(secret, b"", at=now)},
        )
        assert status == 401
    finally:
        server.shutdown()


def test_auth_config_rejects_bad_hmac_scheme_and_tolerance() -> None:
    with pytest.raises(ValueError, match="unknown hmac scheme"):
        AuthConfig(scheme="hmac", hmac_secret="s", hmac_scheme="paypal")
    with pytest.raises(ValueError, match="tolerance must be a positive"):
        AuthConfig(scheme="hmac", hmac_secret="s", hmac_scheme="stripe", hmac_tolerance=0)
