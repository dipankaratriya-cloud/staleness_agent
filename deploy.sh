#!/bin/bash
# One-time setup + deploy script for the staleness pipeline on GCP.
# Usage: bash deploy.sh
set -euo pipefail

PROJECT="datcom-infosys-dev"
REGION="us-central1"
BUCKET="staleness-pipeline-${PROJECT}"
BQ_DATASET="staleness"
IMAGE="gcr.io/${PROJECT}/staleness-pipeline:latest"
SA="staleness-runner@${PROJECT}.iam.gserviceaccount.com"
JOB="staleness-pipeline"
WORKFLOW="staleness-workflow"

echo "▶ Setting project"
gcloud config set project "$PROJECT"

# ── IAM service account ───────────────────────────────────────────────────────
echo "▶ Creating service account"
gcloud iam service-accounts create staleness-runner \
  --display-name "Staleness Pipeline Runner" 2>/dev/null || true

for role in \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/storage.objectAdmin \
  roles/run.invoker; do
  gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="serviceAccount:${SA}" --role="$role" --quiet
done

# ── GCS bucket ────────────────────────────────────────────────────────────────
echo "▶ Creating GCS bucket"
gsutil mb -p "$PROJECT" -l "$REGION" "gs://${BUCKET}" 2>/dev/null || true
gsutil versioning set on "gs://${BUCKET}"   # keeps previous dataset versions

# ── BigQuery dataset ──────────────────────────────────────────────────────────
echo "▶ Creating BigQuery dataset"
bq mk --dataset --location=US "${PROJECT}:${BQ_DATASET}" 2>/dev/null || true

# ── Build + push Docker image ─────────────────────────────────────────────────
echo "▶ Building Docker image"
gcloud builds submit --tag "$IMAGE" .

# ── Deploy Cloud Run Job ──────────────────────────────────────────────────────
echo "▶ Deploying Cloud Run Job"
gcloud run jobs create "$JOB" \
  --image "$IMAGE" \
  --region "$REGION" \
  --service-account "$SA" \
  --memory 8Gi \
  --cpu 4 \
  --task-timeout 86400 \
  --max-retries 1 \
  --set-env-vars "GCS_BUCKET=${BUCKET},GCP_PROJECT=${PROJECT},BQ_DATASET=${BQ_DATASET}" \
  --set-secrets "GITHUB_TOKEN=dipankara-github-token:latest,GROQ_API_KEY=dipankara-groq-api-key:latest,GEMINI_API_KEY=dipankara-gemini-api-key:latest" \
  2>/dev/null || \
gcloud run jobs update "$JOB" \
  --image "$IMAGE" \
  --region "$REGION" \
  --memory 8Gi \
  --cpu 4 \
  --set-env-vars "GCS_BUCKET=${BUCKET},GCP_PROJECT=${PROJECT},BQ_DATASET=${BQ_DATASET}" \
  --set-secrets "GITHUB_TOKEN=dipankara-github-token:latest,GROQ_API_KEY=dipankara-groq-api-key:latest,GEMINI_API_KEY=dipankara-gemini-api-key:latest"

# ── Deploy Cloud Workflow ─────────────────────────────────────────────────────
echo "▶ Deploying Cloud Workflow"
gcloud workflows deploy "$WORKFLOW" \
  --source=workflow.yaml \
  --location="$REGION" \
  --service-account="$SA"

# ── Cloud Scheduler (weekly trigger — every Monday 6 AM UTC) ─────────────────
echo "▶ Creating Cloud Scheduler trigger"
gcloud scheduler jobs create http staleness-weekly \
  --location "$REGION" \
  --schedule "0 6 * * 1" \
  --uri "https://workflowexecutions.googleapis.com/v1/projects/${PROJECT}/locations/${REGION}/workflows/${WORKFLOW}/executions" \
  --message-body '{}' \
  --oauth-service-account-email "$SA" \
  2>/dev/null || true

echo ""
echo "✅ Deploy complete"
echo "   Manual trigger : gcloud workflows run ${WORKFLOW} --location=${REGION}"
echo "   Run logs       : gcloud run jobs executions list --job=${JOB} --region=${REGION}"
echo "   BQ report      : bq query 'SELECT * FROM \`${PROJECT}.${BQ_DATASET}.staleness_report\` ORDER BY run_date DESC LIMIT 50'"
