FROM python:3.12-slim AS base

LABEL maintainer="drt-hub" \
      description="Reverse ETL as code — no UI, no lock-in, no per-row bill."

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

ARG DRT_EXTRAS=""

COPY . /app

RUN if [ -z "$DRT_EXTRAS" ]; then \
      pip install --no-cache-dir .; \
    else \
      pip install --no-cache-dir ".[$DRT_EXTRAS]"; \
    fi

RUN useradd --create-home drt
USER drt

ENTRYPOINT ["drt"]
CMD ["--help"]
