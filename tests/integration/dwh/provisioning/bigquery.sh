#!/usr/bin/env bash
# BigQuery provisioning for the DWH smoke harness (#654 / #673).
#
# Creates a throwaway dataset + a least-privilege service account and mints a
# keyfile. Idempotent-ish (guards on "already exists"). Requires the gcloud CLI
# authenticated to a project you own.
#
# PREREQUISITE: a billing-enabled GCP project. BigQuery jobs require an active
# billing account (= a payment card) even for tiny throwaway tables. If you
# have no card yet, this step is blocked — see the maintainer strategy notes.
#
# Cost: the smoke tables are KB-scale and fit inside BigQuery's free tier
# (1 TB query + 10 GB storage / month), so steady-state cost is ~$0. Set a
# budget alert anyway (below) as a guardrail.
#
# Nothing here is secret except the generated keyfile; keep that out of git.

set -euo pipefail

PROJECT="${DRT_SMOKE_GCP_PROJECT:-drt-smoke}"   # override via env
DATASET="${DRT_SMOKE_BQ_DATASET:-smoke}"
LOCATION="${DRT_SMOKE_BQ_LOCATION:-US}"
SA_NAME="drt-smoke-sa"
SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
KEYFILE="${DRT_SMOKE_BQ_KEYFILE:-./drt-smoke-bq-key.json}"

echo "Project=${PROJECT} Dataset=${DATASET} Location=${LOCATION}"
gcloud config set project "${PROJECT}"
gcloud services enable bigquery.googleapis.com

# ── Throwaway dataset ──────────────────────────────────────────────────────
bq --location="${LOCATION}" mk --dataset "${PROJECT}:${DATASET}" 2>/dev/null \
  || echo "dataset ${DATASET} already exists"

# ── Least-privilege service account ────────────────────────────────────────
gcloud iam service-accounts create "${SA_NAME}" \
  --display-name="drt DWH smoke" 2>/dev/null || echo "SA already exists"

# jobUser at project level (run load/query/MERGE jobs) ...
gcloud projects add-iam-policy-binding "${PROJECT}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser" --condition=None >/dev/null

# ... plus dataEditor scoped to the smoke dataset only (create/insert/drop the
# target + <table>_drt_tmp). Avoid project-wide dataEditor.
bq update --dataset \
  --source <(bq show --format=prettyjson "${PROJECT}:${DATASET}" \
    | python3 -c "import json,sys;d=json.load(sys.stdin);d.setdefault('access',[]).append({'role':'WRITER','userByEmail':'${SA_EMAIL}'});print(json.dumps(d))") \
  "${PROJECT}:${DATASET}"

# ── Keyfile (keep OUT of git; load into SMOKE_BIGQUERY_KEYFILE_JSON secret) ──
gcloud iam service-accounts keys create "${KEYFILE}" --iam-account="${SA_EMAIL}"
echo "Keyfile written to ${KEYFILE} — register its CONTENTS as SMOKE_BIGQUERY_KEYFILE_JSON, then delete the local file."

# ── Budget alert guardrail (optional; needs billing account id) ─────────────
# gcloud billing budgets create --billing-account=<ACCOUNT_ID> \
#   --display-name="drt-smoke" --budget-amount=5USD ...

cat <<EOF

Secret mapping (register as repo secrets):
  SMOKE_BIGQUERY_PROJECT      = ${PROJECT}
  SMOKE_BIGQUERY_DATASET      = ${DATASET}
  SMOKE_BIGQUERY_KEYFILE_JSON = <contents of ${KEYFILE}>

Caveat: if the org enforces iam.disableServiceAccountKeyCreation, key creation
fails — provision in a project/folder where that policy isn't enforced.
EOF
