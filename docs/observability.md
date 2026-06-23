# Observability (OpenTelemetry tracing)

drt can emit [OpenTelemetry](https://opentelemetry.io/) **traces** for every
sync it runs. Tracing is opt-in and zero-cost when disabled: the engine always
calls into a tracer, but with no exporter configured (or the `[otel]` extra not
installed) that tracer is a no-op and adds no measurable overhead.

## TL;DR

```bash
# 1. install the extra
pip install "drt-core[otel]"

# 2. point drt at an OTLP collector — either in ~/.drt/profiles.yml ...
#    observability:
#      otel:
#        endpoint: "localhost:4317"
#
# 3. ... or via environment variables
export OTEL_EXPORTER_OTLP_ENDPOINT="localhost:4317"

# 4. run as usual — spans are exported per sync
drt run
```

If the `[otel]` extra is not installed, or no endpoint is configured, drt runs
exactly as before and exports nothing.

## Enabling export

drt exports over **OTLP/gRPC**. Configure it in one of two ways.

### 1. `~/.drt/profiles.yml` (preferred)

Add a top-level `observability` block alongside your profiles:

```yaml
observability:
  otel:
    endpoint: "localhost:4317"     # OTLP/gRPC endpoint of your collector
    service_name: "drt"            # optional, defaults to "drt"
    headers:                        # optional, sent with every export
      authorization: "Bearer ${OTEL_TOKEN}"
```

- `endpoint` — the only required field. When it is absent, drt stays in no-op
  mode and exports nothing.
- `service_name` — the `service.name` resource attribute. Defaults to `drt`.
- `headers` — arbitrary export headers (e.g. auth for a hosted backend).
  Values support `${VAR}` environment-variable substitution, so secrets stay
  out of the file.

A scheme-less endpoint such as `localhost:4317` uses the default gRPC TLS
convention. Prefix with `http://` (e.g. `http://localhost:4317`) for a local,
insecure collector.

### 2. Environment variables

If no `observability` block is present, drt falls back to the standard OTLP
environment variables:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="localhost:4317"
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer abc123,x-tenant=acme"
```

`OTEL_EXPORTER_OTLP_HEADERS` is a comma-separated list of `key=value` pairs.

## What gets traced

Each sync produces one trace with this shape:

```
drt.sync.run                       attrs: sync.name, source.type,
│                                         destination.type, sync.mode, batch_size
│                                  status: OK on completion,
│                                          ERROR (+ recorded exception) on an
│                                          uncaught failure
├── drt.sync.extract               attrs: extract.rows_extracted
│
├── drt.sync.load   (batch 0)      attrs: batch_index, batch_size,
├── drt.sync.load   (batch 1)             load.success, load.failed, load.skipped
└── drt.sync.load   (batch N)
```

- **`drt.sync.run`** wraps the whole sync. Its status is `OK` on a clean return
  and `ERROR` (with the exception recorded on the span) when the sync raises.
- **`drt.sync.extract`** is a child of `drt.sync.run` covering source
  extraction. Because extraction is streamed and interleaved with loading, this
  span runs alongside the load spans rather than strictly before them; its
  `extract.rows_extracted` attribute is the total number of rows pulled.
- **`drt.sync.load`** is emitted once per batch handed to `destination.load()`,
  carrying the batch position and the per-batch outcome counts from the returned
  `SyncResult`. Staged destinations (`stage()`/`finalize()`) are not wrapped by a
  load span.

When `--threads` runs syncs in parallel, each sync executes on its own thread
and produces its own independent `drt.sync.run` trace.

## Trying it locally with Jaeger

[Jaeger](https://www.jaegertracing.io/) all-in-one ships an OTLP receiver and a
UI, which makes it a quick way to see drt traces.

```yaml
# docker-compose.observability.yml
services:
  jaeger:
    image: jaegertracing/all-in-one:1.57
    ports:
      - "16686:16686"   # Jaeger UI
      - "4317:4317"     # OTLP/gRPC receiver
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
```

```bash
docker compose -f docker-compose.observability.yml up -d

export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317"
drt run

# open the UI and pick the "drt" service
open http://localhost:16686
```

You should see a `drt.sync.run` trace per sync, with an `extract` span and one
`load` span per batch nested underneath.

## Disabling

Tracing is off whenever there is no configured endpoint. To turn it off, remove
the `observability` block from `profiles.yml` and unset
`OTEL_EXPORTER_OTLP_ENDPOINT`. You can also simply install drt without the
`[otel]` extra — the engine falls back to a no-op tracer and never attempts to
export.