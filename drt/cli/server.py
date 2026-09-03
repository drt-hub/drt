"""drt webhook server — lightweight HTTP endpoint to trigger syncs.

Enables event-driven sync patterns (GitHub webhooks, dbt job completion,
Pub/Sub push behind a proxy, etc.). No external dependencies — uses stdlib
``http.server``.

Routes:
    GET  /health                → {"status": "ok", "version": "..."}
    POST /sync/<name>           → 202 {"run_id": ...} — accepted, runs in background
    POST /sync/<name>?wait=true → run synchronously, return SyncResult (200/207)
    POST /sync/<name>?dry_run=true  → dry run (combines with wait)
    GET  /runs/<id>             → run state + result once finished

Concurrency contract (#854) — this is a promise, not an implementation detail:
    - **A trigger accepted with 202 will run.** Nothing accepted is dropped.
    - **Different syncs run concurrently.** Only same-sync overlap is serialized.
    - **Same-sync triggers coalesce to depth 1.** While a sync is running, at
      most one pending run exists per sync. Any trigger that does not start a
      run of its own is answered with that pending run's id and
      ``"coalesced": true``, whether it created the pending run or joined one
      already there. Those two cases are deliberately indistinguishable to the
      caller: drt syncs are watermark-driven, so the pending run picks up
      everything that accumulated, and a queue of N would do the same work as
      a queue of 1.
    - Dry-run and real triggers coalesce **separately**: folding a real trigger
      into a pending dry-run preview would silently discard the write.

Run ids are valid for the lifetime of this ``drt serve`` process — they are
held in memory, so ``GET /runs/<id>`` goes blank after a restart. #762 (first-
class run_id) is the upgrade path to durable ids; the endpoint contract will
not change, only the id's durability.

Auth (``--auth`` on ``drt serve``):
    none    unauthenticated (local dev only)
    bearer  static token compared with ``secrets.compare_digest``
    hmac    a request signature, in one of two shapes selected by
            ``--hmac-scheme``:
              generic  HMAC-SHA256 of the raw body, GitHub-style
                       (``X-Hub-Signature-256: sha256=<hex>``; bare hex and
                       base64 digests are also accepted, covering Shopify)
              stripe   ``Stripe-Signature: t=<ts>,v1=<hex>`` over ``<ts>.<body>``
                       with a replay window (#969). POST only — Stripe never
                       sends a GET, so ``GET /runs/<id>`` has no signature to
                       verify under this scheme.

Every route except ``GET /health`` is authenticated. ``GET /runs/<id>`` returns
the SyncResult and any error text, so it is not exempt: the run id is a uuid4,
but it travels in the URL path, which every reverse proxy in front of drt writes
to an access log. Under ``hmac`` a GET has no body to sign, so the signature is
over the empty body, making it a constant per secret (see the guide).

Pub/Sub push authenticates with an OIDC JWT instead of a body signature; that
verification path is a follow-up (#903 — it needs a JWT dependency decision) and until
then Pub/Sub still requires a verifying proxy in front.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import itertools
import json
import secrets
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs

from drt import __version__

# Webhook payloads are small; a larger body is a mistake or abuse.
_MAX_BODY_BYTES = 1024 * 1024
# Finished runs kept for GET /runs/<id>; oldest evicted beyond this.
_MAX_FINISHED_RUNS = 1000


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Auth


@dataclass(frozen=True)
class AuthConfig:
    """Which verification scheme the endpoint expects (#854).

    Schemes are dispatched rather than hardcoded so a later verifier (e.g.
    OIDC for Pub/Sub push) is an addition, not a rewrite.
    """

    scheme: str = "none"  # "none" | "bearer" | "hmac"
    token: str | None = None
    hmac_secret: str | None = None
    hmac_header: str = "X-Hub-Signature-256"
    # Which signature *shape* the hmac scheme expects (#969). "generic" is the
    # #854 behaviour verbatim — a digest of the body in any of three encodings,
    # which covers GitHub, Shopify and bare-hex senders. "stripe" is a different
    # construction, not another encoding of the same one, so it is a separate
    # verifier rather than a fourth candidate in the same tuple.
    hmac_scheme: str = "generic"  # "generic" | "stripe"
    # Replay window for timestamped schemes, in seconds. Stripe's own libraries
    # default to five minutes; their docs warn against 0, which turns the
    # recency check off entirely rather than making it strict.
    hmac_tolerance: int = 300

    def __post_init__(self) -> None:
        if self.scheme not in ("none", "bearer", "hmac"):
            raise ValueError(f"unknown auth scheme: {self.scheme!r}")
        if self.scheme == "bearer" and not self.token:
            raise ValueError("auth scheme 'bearer' requires a token")
        if self.scheme == "hmac" and not self.hmac_secret:
            raise ValueError("auth scheme 'hmac' requires a secret")
        if self.hmac_scheme not in ("generic", "stripe"):
            raise ValueError(f"unknown hmac scheme: {self.hmac_scheme!r}")
        if self.hmac_tolerance <= 0:
            raise ValueError(
                "hmac tolerance must be a positive number of seconds; 0 would disable "
                "the replay-window check rather than tighten it"
            )


def _verify_bearer(header_value: str, token: str) -> bool:
    return secrets.compare_digest(header_value.encode(), f"Bearer {token}".encode())


_GET_KEY_INFO = b"drt/serve/v1/get-path"


def _derive_get_key(secret: str) -> bytes:
    """Derive the separate key GET path signatures are computed under.

    A POST signature has to stay ``HMAC(secret, raw_body)`` because that is what
    an external sender (GitHub, Shopify) already computes over the body it
    sends. A GET has no body, so its signature covers the request path instead.

    Those two payloads must never be signed under the same key: the attacker
    picks a POST body, so any deterministic GET payload can simply be sent
    *as* a body, and one captured GET signature would authenticate an arbitrary
    ``POST /sync/<name>``. Separate keys make that collision impossible rather
    than merely unlikely — no prefix or separator can, since a body is free
    bytes.
    """
    return hmac.new(secret.encode(), _GET_KEY_INFO, hashlib.sha256).digest()


def _verify_hmac(body: bytes, header_value: str, secret: str, *, path: str | None = None) -> bool:
    """Check an HMAC-SHA256 signature in any of the common encodings.

    GitHub sends ``sha256=<hex>``, Shopify sends base64, and a hand-rolled
    sender may send bare hex — all three are digests of the same bytes, so
    accepting each costs nothing. Stripe signs something else entirely — a
    timestamped ``t=...,v1=...`` construction with a replay window; see
    :func:`_verify_stripe_hmac`.

    ``path`` is passed for GET only, and switches the signature to cover the
    request path under :func:`_derive_get_key` instead of the body. It is keyed
    off the HTTP method, never off "the body happens to be empty" — ``POST
    /sync/<name>`` is documented as bodyless, and its signature is still over
    ``b""`` under the raw secret.
    """
    if path is not None:
        digest = hmac.new(_derive_get_key(secret), path.encode(), hashlib.sha256)
    else:
        digest = hmac.new(secret.encode(), body, hashlib.sha256)
    accepted = (
        f"sha256={digest.hexdigest()}",
        digest.hexdigest(),
        base64.b64encode(digest.digest()).decode(),
    )
    got = header_value.encode()
    return any(secrets.compare_digest(got, candidate.encode()) for candidate in accepted)


def _verify_stripe_hmac(
    body: bytes, header_value: str, secret: str, tolerance: int, now: float | None = None
) -> bool:
    """Verify a Stripe ``Stripe-Signature`` header (#969).

    Stripe's scheme differs from GitHub/Shopify in three ways that each matter:

    * the signed payload is ``f"{t}.{body}"``, not the body alone, so the
      timestamp is covered by the signature and cannot be edited by a replayer;
    * the header carries several comma-separated ``prefix=value`` elements, and
      **more than one may be ``v1``** — rolling an endpoint secret leaves the
      old one live for up to 24h and Stripe signs once per active secret, so
      rejecting on the first mismatch breaks every rotation;
    * a signature is only fresh for a window. Stripe's libraries default to five
      minutes.

    Only ``v1`` is verified. Stripe attaches a deliberately fake ``v0`` to test
    events and its docs are explicit — "to prevent downgrade attacks, ignore all
    schemes that aren't v1" — so accepting any ``v*`` that happens to match
    would be a real vulnerability, not a convenience.

    ``now`` is injectable purely so the replay window can be tested without
    sleeping or freezing the process clock.
    """
    timestamp: str | None = None
    signatures: list[str] = []
    for element in header_value.split(","):
        prefix, _, value = element.strip().partition("=")
        if prefix == "t":
            # A second `t=` would make "which timestamp was signed?" ambiguous;
            # Stripe sends exactly one, so treat anything else as malformed.
            if timestamp is not None:
                return False
            timestamp = value
        elif prefix == "v1":
            signatures.append(value)

    if timestamp is None or not signatures:
        return False

    # Symmetric window. Stripe's own libraries only reject *stale* timestamps,
    # which leaves a far-future one valid indefinitely; bounding both directions
    # costs nothing and clock skew is bidirectional anyway.
    #
    # The arithmetic is inside the `try`, not just the parse: Python ints are
    # arbitrary precision, so `t=<400 digits>` parses fine and then raises
    # OverflowError on the float comparison. Every input here is attacker-chosen
    # and unauthenticated, so a malformed one must return False, never raise.
    try:
        signed_at = int(timestamp)
        current = time.time() if now is None else now
        if abs(current - signed_at) > tolerance:
            return False
    except (ValueError, OverflowError):
        return False

    expected = (
        hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
        .hexdigest()
        .encode()
    )
    # Compared as bytes, matching `_verify_hmac`: `secrets.compare_digest` rejects
    # non-ASCII `str` operands with a TypeError, and `v1=<non-ascii>` is free for
    # an unauthenticated client to send.
    #
    # Every candidate is compared even after a match: `any()` would short-circuit
    # and leak, through timing, how many signatures were tried.
    return sum(secrets.compare_digest(expected, candidate.encode()) for candidate in signatures) > 0


# ---------------------------------------------------------------------------
# Runs + scheduling


@dataclass
class _Run:
    """One triggered execution, from acceptance to terminal state."""

    run_id: str
    sync_name: str
    dry_run: bool
    seq: int
    state: str = "pending"  # pending → running → success | partial | failed | error
    created_at: str = field(default_factory=_utcnow)
    started_at: str | None = None
    finished_at: str | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    done: threading.Event = field(default_factory=threading.Event)

    def to_json(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "sync_name": self.sync_name,
            "state": self.state,
            "dry_run": self.dry_run,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "result": self.result,
            "error": self.error,
        }


class SyncScheduler:
    """Per-sync execution with depth-1 coalescing — the #854 contract.

    One lock guards all bookkeeping; run state transitions happen under it so
    a concurrent ``GET /runs/<id>`` never sees a half-written run.
    """

    def __init__(
        self,
        runner: Callable[[str, bool], dict[str, Any]],
        max_finished: int = _MAX_FINISHED_RUNS,
    ) -> None:
        self._runner = runner
        self._lock = threading.Lock()
        self._seq = itertools.count()
        self._runs: dict[str, _Run] = {}  # run_id → run (insertion-ordered)
        self._running: dict[str, _Run] = {}  # sync_name → running run
        self._pending: dict[tuple[str, bool], _Run] = {}  # (sync_name, dry_run) → pending
        self._max_finished = max_finished

    def get(self, run_id: str) -> _Run | None:
        with self._lock:
            return self._runs.get(run_id)

    def trigger(self, sync_name: str, dry_run: bool) -> tuple[_Run, bool]:
        """Accept a trigger; return ``(run, coalesced)``.

        ``coalesced`` answers "was this trigger denied a run of its own?", not
        "did it join a pending run that already existed". The trigger that
        creates the pending run gets ``True`` as well, and a caller cannot tell
        the two apart.

        Idle sync → new run starts immediately (coalesced=False).
        Running sync, no pending of this kind → new pending run (coalesced=True).
        Running sync, pending exists → that same pending run (coalesced=True).
        """
        with self._lock:
            if sync_name not in self._running:
                run = self._new_run_locked(sync_name, dry_run)
                self._start_locked(run)
                return run, False
            key = (sync_name, dry_run)
            existing = self._pending.get(key)
            if existing is not None:
                return existing, True
            run = self._new_run_locked(sync_name, dry_run)
            self._pending[key] = run
            return run, True

    def _new_run_locked(self, sync_name: str, dry_run: bool) -> _Run:
        run = _Run(
            run_id=uuid.uuid4().hex,
            sync_name=sync_name,
            dry_run=dry_run,
            seq=next(self._seq),
        )
        self._runs[run.run_id] = run
        self._evict_locked()
        return run

    def _evict_locked(self) -> None:
        # "Finished" is ``finished_at``, not a whitelist of state strings: the
        # state comes from the runner's result contract, and a status added
        # there later would otherwise make the run permanently un-evictable.
        # ``finished_at`` is set under this lock for every run that completes.
        finished = [r for r in self._runs.values() if r.finished_at is not None]
        for run in finished[: max(0, len(finished) - self._max_finished)]:
            del self._runs[run.run_id]

    def _start_locked(self, run: _Run) -> None:
        self._running[run.sync_name] = run
        run.state = "running"
        run.started_at = _utcnow()
        threading.Thread(
            target=self._execute,
            args=(run,),
            daemon=True,
            name=f"drt-run-{run.run_id[:8]}",
        ).start()

    def _execute(self, run: _Run) -> None:
        try:
            result = self._runner(run.sync_name, run.dry_run)
            with self._lock:
                run.result = result
                run.state = result.get("status", "success")
        except Exception as e:
            with self._lock:
                run.error = str(e)
                run.state = "error"
        finally:
            with self._lock:
                run.finished_at = _utcnow()
                del self._running[run.sync_name]
                nxt = self._pop_next_pending_locked(run.sync_name)
                if nxt is not None:
                    self._start_locked(nxt)
            run.done.set()

    def _pop_next_pending_locked(self, sync_name: str) -> _Run | None:
        # At most two pendings per sync (one dry, one real); oldest first.
        candidates = [r for (name, _), r in self._pending.items() if name == sync_name]
        if not candidates:
            return None
        nxt = min(candidates, key=lambda r: r.seq)
        del self._pending[(sync_name, nxt.dry_run)]
        return nxt


# ---------------------------------------------------------------------------
# HTTP layer


def make_handler(
    auth: AuthConfig,
    scheduler: SyncScheduler,
    sync_exists: Callable[[str], bool],
) -> type[BaseHTTPRequestHandler]:
    """Build a request handler bound to the given auth config and scheduler."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            # Quiet the default stderr access log
            return

        def _json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self) -> bytes | None:
            """Read the request body; ``None`` means it was too large (413 sent)."""
            length = int(self.headers.get("Content-Length") or 0)
            if length > _MAX_BODY_BYTES:
                self._json(413, {"error": "request body too large"})
                return None
            return self.rfile.read(length) if length else b""

        def _check_auth(self, body: bytes, *, path: str | None = None) -> bool:
            # `path` is set by do_GET only. Passing it switches the hmac scheme
            # to sign the request path under a separate key; leaving it None
            # keeps the body-signing contract every POST sender already uses.
            if auth.scheme == "none":
                return True
            if auth.scheme == "bearer":
                assert auth.token is not None
                return _verify_bearer(self.headers.get("Authorization", ""), auth.token)
            assert auth.hmac_secret is not None
            signature = self.headers.get(auth.hmac_header, "")
            if not signature:
                return False
            if auth.hmac_scheme == "stripe":
                # POST only, by design (#969). Stripe never sends a GET, so
                # there is no Stripe-Signature to verify on one and no payload
                # shape Stripe would define for it. Verifying a GET against the
                # empty body would also hand back exactly what #998 removed: a
                # signature that is independent of the path it was issued for,
                # and so replayable across run ids for the whole tolerance
                # window. Reject rather than invent a convention.
                if path is not None:
                    return False
                return _verify_stripe_hmac(body, signature, auth.hmac_secret, auth.hmac_tolerance)
            return _verify_hmac(body, signature, auth.hmac_secret, path=path)

        def do_GET(self) -> None:  # noqa: N802
            # /health answers before the auth check so a load-balancer probe
            # needs no credential. Nothing else is exempt: GET /runs/<id>
            # returns the SyncResult and the error string, and the run id stops
            # being a secret the moment a proxy logs the URL path. A GET has no
            # body, so the hmac scheme signs the request path instead — binding
            # the signature to the run id it was issued for, so a leaked header
            # cannot read a different one.
            if self.path == "/health":
                self._json(200, {"status": "ok", "version": __version__})
                return
            if not self._check_auth(b"", path=self.path):
                self._json(401, {"error": "unauthorized"})
                return
            if self.path.startswith("/runs/"):
                run_id = self.path[len("/runs/") :].strip("/")
                run = scheduler.get(run_id)
                if run is None:
                    self._json(
                        404,
                        {
                            "error": "unknown run id — ids are valid for the "
                            "lifetime of this drt serve process"
                        },
                    )
                    return
                self._json(200, run.to_json())
                return
            self._json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            body = self._read_body()
            if body is None:
                return
            if not self._check_auth(body):
                self._json(401, {"error": "unauthorized"})
                return

            path, _, query = self.path.partition("?")
            if not path.startswith("/sync/"):
                self._json(404, {"error": "not found"})
                return

            sync_name = path[len("/sync/") :].strip("/")
            if not sync_name:
                self._json(400, {"error": "sync name required"})
                return

            params = parse_qs(query)
            dry_run = params.get("dry_run", ["false"])[0].lower() == "true"
            wait = params.get("wait", ["false"])[0].lower() == "true"

            try:
                if not sync_exists(sync_name):
                    self._json(404, {"error": f"No sync named '{sync_name}' found"})
                    return
            except Exception as e:
                self._json(500, {"error": f"project load failed: {e}"})
                return

            run, coalesced = scheduler.trigger(sync_name, dry_run)

            if not wait:
                self._json(
                    202,
                    {
                        "run_id": run.run_id,
                        "sync_name": sync_name,
                        "state": run.state,
                        "dry_run": dry_run,
                        "coalesced": coalesced,
                        "url": f"/runs/{run.run_id}",
                    },
                )
                return

            # ?wait=true — the pre-#854 synchronous contract. If the trigger
            # coalesced, this blocks until the pending run has executed too.
            run.done.wait()
            if run.state == "error":
                self._json(500, {"error": f"sync failed: {run.error}"})
                return
            assert run.result is not None
            self._json(200 if run.state == "success" else 207, run.result)

    return Handler


# ---------------------------------------------------------------------------
# Entry point


def serve(
    host: str = "127.0.0.1",
    port: int = 8080,
    token: str | None = None,
    project_dir: str = ".",
    auth_scheme: str = "auto",
    hmac_secret: str | None = None,
    hmac_header: str = "X-Hub-Signature-256",
    hmac_scheme: str = "generic",
    hmac_tolerance: int = 300,
) -> None:
    """Start the webhook server (blocking).

    ``auth_scheme="auto"`` keeps the historical default: bearer when a token
    is set, unauthenticated otherwise.
    """
    from pathlib import Path

    from drt.config.base import ProjectConfig
    from drt.config.parser import load_project
    from drt.state.factory import build_state_bundle

    if auth_scheme == "auto":
        auth_scheme = "bearer" if token else "none"
    auth = AuthConfig(
        scheme=auth_scheme,
        token=token,
        hmac_secret=hmac_secret,
        hmac_header=hmac_header,
        hmac_scheme=hmac_scheme,
        hmac_tolerance=hmac_tolerance,
    )

    project_path = Path(project_dir)
    project = (
        load_project(project_path)
        if (project_path / "drt_project.yml").exists()
        else ProjectConfig(name="drt")
    )
    state_manager = build_state_bundle(project, project_path).state

    def runner(sync_name: str, dry_run: bool) -> dict[str, Any]:
        from drt.integrations._runner import run_drt_sync

        return run_drt_sync(
            sync_name=sync_name,
            project_dir=project_dir,
            dry_run=dry_run,
            state_manager=state_manager,
        )

    def sync_exists(name: str) -> bool:
        from drt.config.parser import load_syncs

        return any(s.name == name for s in load_syncs(Path(project_dir)))

    scheduler = SyncScheduler(runner)
    handler = make_handler(auth, scheduler, sync_exists)
    server = ThreadingHTTPServer((host, port), handler)
    auth_note = {
        "none": "[yellow]without auth (local dev only)[/yellow]",
        "bearer": "with bearer token auth",
        "hmac": (
            f"with Stripe signature auth ({auth.hmac_header}, {auth.hmac_tolerance}s window)"
            if auth.hmac_scheme == "stripe"
            else f"with HMAC auth ({auth.hmac_header})"
        ),
    }[auth.scheme]
    from drt.cli.output import console

    console.print(f"[bold]drt webhook server[/bold] listening on http://{host}:{port} {auth_note}")
    console.print("  POST /sync/<name>[?dry_run=true][&wait=true]  → trigger sync (202 + run id)")
    console.print("  GET  /runs/<id>                    → run state")
    console.print("  GET  /health                       → status")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        console.print("\n[dim]Shutting down... (in-flight runs are abandoned)[/dim]")
        server.shutdown()
