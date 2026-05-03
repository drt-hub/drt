# Graceful Shutdown — SIGTERM/SIGINT Handling

In containerized and orchestrated environments (Docker, Kubernetes, Airflow, ECS), the runtime sends `SIGTERM` to processes before forcefully killing them. Without graceful shutdown, in-flight syncs can leave corrupted state and duplicate records on the next run.

`drt run` handles `SIGTERM` and `SIGINT` cooperatively: it finishes the current batch, persists state and watermark, then exits with the appropriate POSIX exit code.

## Behavior

When `drt run` receives `SIGTERM` or `SIGINT`:

1. **Finish the current batch.** The signal sets a `stop_event`; the engine checks it between batches. Whatever rows are mid-flight in the current batch complete normally.
2. **Persist state.** Successful records are saved. The watermark advances to the highest cursor value seen across completed batches.
3. **Exit with a POSIX-conventional code:**
   - `SIGINT` (Ctrl+C): exit code **130** (`128 + 2`)
   - `SIGTERM` (orchestrator stop): exit code **143** (`128 + 15`)
4. **30-second force-exit watchdog.** If the current batch hangs (e.g. an unresponsive destination), drt force-exits after 30 seconds with the same exit code. Operators can rely on a bounded shutdown window when sizing K8s `terminationGracePeriodSeconds`.

A second signal (e.g. user double-tapping Ctrl+C) is a no-op — the watchdog only starts once.

## What does *not* happen

- **No mid-batch interruption.** drt does not abort an in-flight HTTP call or DB transaction. This is intentional: half-completed batches are the source of duplication and corruption that this feature exists to prevent.
- **No state loss.** Even if the watchdog force-exits, all batches that completed before the signal arrived are persisted.
- **No re-run on `--threads N`.** Each worker checks the same `stop_event`; once it fires, no new batches start in any worker.

## Kubernetes example

```yaml
apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      terminationGracePeriodSeconds: 60   # leave headroom over drt's 30s watchdog
      containers:
        - name: drt
          image: my-registry/drt:0.7.0
          command: ["drt", "run"]
```

When the pod is evicted or the job is deleted, kubelet sends `SIGTERM`, drt finishes its current batch (≤30s thanks to the watchdog), and the pod exits cleanly.

## Airflow / Prefect

Both orchestrators send `SIGTERM` on task cancellation. drt's exit code 143 is recognized as a non-zero exit by both, so the task is marked failed and can be retried — the next run resumes from the watermark drt persisted before exit.

## Verifying behavior locally

```bash
# Terminal 1
drt run

# Terminal 2 — find PID and send SIGTERM
pkill -TERM -f "drt run"
echo $?   # 143
```

Or send SIGINT with Ctrl+C in Terminal 1 → exit code 130.

## Implementation notes

- Signal handlers are registered only for `drt run` — other commands (`drt validate`, `drt list`, etc.) keep Python's default handlers.
- Handlers run in the main thread per Python convention; the `stop_event` is a `threading.Event` that's safely visible to worker threads in `--threads N` mode.
- The watchdog is a `threading.Timer(30.0, os._exit)`, cancelled if drt completes the current batch in time.
