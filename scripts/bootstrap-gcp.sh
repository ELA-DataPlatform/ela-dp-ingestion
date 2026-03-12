#!/bin/bash
# Bootstrap GCP infrastructure for CI/CD (run once per environment)
# Usage: ./scripts/bootstrap-gcp.sh dev
#        ./scripts/bootstrap-gcp.sh prd
set -euo pipefail

ENV="${1:?Usage: $0 <dev|prd>}"
if [[ "$ENV" != "dev" && "$ENV" != "prd" ]]; then
  echo "Error: env must be 'dev' or 'prd'"
  exit 1
fi

PROJECT="ela-dp-${ENV}"
REGION="europe-west1"
AR_REPO="ela-dp-ingestion"
SA_NAME="github-actions-ingestion"
GITHUB_ORG="ELA-DataPlatform"       # <-- à remplacer
GITHUB_REPO="ela-dp-ingestion"
WIF_POOL="github-pool"
WIF_PROVIDER="github-provider"

echo "==> Configuring project: $PROJECT"
gcloud config set project "$PROJECT"

# 1. Enable required APIs
echo "==> Enabling APIs..."
gcloud services enable \
  artifactregistry.googleapis.com \
  run.googleapis.com \
  iam.googleapis.com \
  iamcredentials.googleapis.com

# 2. Create Artifact Registry repository
echo "==> Creating Artifact Registry repo..."
gcloud artifacts repositories create "$AR_REPO" \
  --repository-format=docker \
  --location="$REGION" \
  --description="ela-dp ingestion images" \
  || echo "  (already exists, skipping)"

# 3. Create service account for GitHub Actions
echo "==> Creating service account..."
gcloud iam service-accounts create "$SA_NAME" \
  --display-name="GitHub Actions — ela-dp-ingestion" \
  || echo "  (already exists, skipping)"

SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"

# 4. Grant required roles to the service account
echo "==> Granting roles..."
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.developer"

gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/iam.serviceAccountUser"

# 5. Create Workload Identity Pool
echo "==> Creating Workload Identity Pool..."
gcloud iam workload-identity-pools create "$WIF_POOL" \
  --location="global" \
  --display-name="GitHub Actions pool" \
  || echo "  (already exists, skipping)"

WIF_POOL_ID=$(gcloud iam workload-identity-pools describe "$WIF_POOL" \
  --location="global" \
  --format="value(name)")

# 6. Create Workload Identity Provider
echo "==> Creating Workload Identity Provider..."
gcloud iam workload-identity-pools providers create-oidc "$WIF_PROVIDER" \
  --location="global" \
  --workload-identity-pool="$WIF_POOL" \
  --display-name="GitHub provider" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository=='${GITHUB_ORG}/${GITHUB_REPO}'" \
  || echo "  (already exists, skipping)"

WIF_PROVIDER_ID=$(gcloud iam workload-identity-pools providers describe "$WIF_PROVIDER" \
  --location="global" \
  --workload-identity-pool="$WIF_POOL" \
  --format="value(name)")

# 7. Allow GitHub Actions to impersonate the service account
echo "==> Binding Workload Identity to service account..."
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${WIF_POOL_ID}/attribute.repository/${GITHUB_ORG}/${GITHUB_REPO}"

# 8. Print GitHub secrets to set
echo ""
echo "=========================================="
echo "Setup complete for project: $PROJECT"
echo "=========================================="
echo ""
echo "Set these GitHub Actions secrets:"
if [[ "$ENV" == "dev" ]]; then
  echo "  WIF_PROVIDER_DEV       = ${WIF_PROVIDER_ID}"
  echo "  WIF_SERVICE_ACCOUNT_DEV = ${SA_EMAIL}"
else
  echo "  WIF_PROVIDER_PRD       = ${WIF_PROVIDER_ID}"
  echo "  WIF_SERVICE_ACCOUNT_PRD = ${SA_EMAIL}"
fi
echo ""
echo "Add secrets via:"
echo "  gh secret set WIF_PROVIDER_${ENV^^} --body \"${WIF_PROVIDER_ID}\""
echo "  gh secret set WIF_SERVICE_ACCOUNT_${ENV^^} --body \"${SA_EMAIL}\""
