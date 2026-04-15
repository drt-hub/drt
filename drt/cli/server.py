"""drt webhook server — lightweight HTTP endpoint to trigger syncs.

Enables event-driven sync patterns (GitHub webhooks, dbt job completion, etc.).
No external dependencies — uses stdlib ``http.server``.

Routes:
    GET  /health           → {"status": "ok", "version": "..."}
    POST /sync/<name>      → run sync, return SyncResult as JSON
    POST /sync/<name>?dry_run=true  → dry run

Auth:
    Optional Bearer token via ``token`` argument (from ``DRT_WEBHOOK_TOKEN``
    env var by default). If not set, endpoint is unauthenticated (local dev).

One sync at a time — concurrent requests get 423 Locked.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from drt import __version__


class _SyncLock:
    """Serialize sync executions — one at a time."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def try_acquire(self) -> bool:
        return self._lock.acquire(blocking=False)

    def release(self) -> None:
        self._lock.release()


def make_handler(
    token: str | None,
    sync_lock: _SyncLock,
    project_dir: str = ".",
) -> type[BaseHTTPRequestHandler]:
    """Build a request handler bound to the given token and project dir."""

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

        def _check_auth(self) -> bool:
            if token is None:
                return True
            header = self.headers.get("Authorization", "")
            return header == f"Bearer {token}"

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self._json(200, {"status": "ok", "version": __version__})
                return
            self._json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if not self._check_auth():
                self._json(401, {"error": "unauthorized"})
                return

            path, _, query = self.path.partition("?")
            if not path.startswith("/sync/"):
                self._json(404, {"error": "not found"})
                return

            sync_name = path[len("/sync/"):].strip("/")
            if not sync_name:
                self._json(400, {"error": "sync name required"})
                return

            dry_run = "dry_run=true" in query

            if not sync_lock.try_acquire():
                self._json(
                    423,
                    {"error": "another sync is already running"},
                )
                return

            try:
                from drt.integrations._runner import run_drt_sync

                result = run_drt_sync(
                    sync_name=sync_name,
                    project_dir=project_dir,
                    dry_run=dry_run,
                )
                status = 200 if result["status"] == "success" else 207
                self._json(status, result)
            except ValueError as e:
                self._json(404, {"error": str(e)})
            except Exception as e:
                self._json(
                    500,
                    {"error": f"sync failed: {e}"},
                )
            finally:
                sync_lock.release()

    return Handler


def serve(
    host: str = "127.0.0.1",
    port: int = 8080,
    token: str | None = None,
    project_dir: str = ".",
) -> None:
    """Start the webhook server (blocking)."""
    sync_lock = _SyncLock()
    handler = make_handler(token, sync_lock, project_dir)
    server = ThreadingHTTPServer((host, port), handler)
    auth_note = (
        "with bearer token auth"
        if token
        else "[yellow]without auth (local dev only)[/yellow]"
    )
    from drt.cli.output import console

    console.print(
        f"[bold]drt webhook server[/bold] listening on "
        f"http://{host}:{port} {auth_note}"
    )
    console.print("  POST /sync/<name>[?dry_run=true]  → trigger sync")
    console.print("  GET  /health                       → status")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        console.print("\n[dim]Shutting down...[/dim]")
        server.shutdown()