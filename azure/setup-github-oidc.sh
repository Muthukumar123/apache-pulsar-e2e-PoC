#!/usr/bin/env bash
set -euo pipefail

REPO="${GITHUB_REPOSITORY:-Muthukumar123/apache-pulsar-e2e-PoC}"
REPO_OWNER="${REPO%%/*}"
REPO_NAME="${REPO##*/}"
REPO_OWNER_ID="${GITHUB_REPOSITORY_OWNER_ID:-3875126}"
REPO_ID="${GITHUB_REPOSITORY_ID:-1372062775}"
SUBSCRIPTION_ID="${AZURE_SUBSCRIPTION_ID:-$(az account show --query id -o tsv)}"
TENANT_ID="${AZURE_TENANT_ID:-$(az account show --query tenantId -o tsv)}"
APP_NAME="${AZURE_APP_NAME:-github-${REPO_NAME}-deploy}"
ROLE="${AZURE_ROLE:-Contributor}"

command -v az >/dev/null || { echo "Azure CLI (az) is required"; exit 1; }
az account show >/dev/null 2>&1 || { echo "Run az login first"; exit 1; }

az account set --subscription "$SUBSCRIPTION_ID"

echo "Creating/reusing Entra application: $APP_NAME"
APP_ID="$(az ad app list --display-name "$APP_NAME" --query '[0].appId' -o tsv)"
if [[ -z "$APP_ID" ]]; then
  APP_ID="$(az ad app create --display-name "$APP_NAME" --query appId -o tsv)"
fi

APP_OBJECT_ID="$(az ad app show --id "$APP_ID" --query id -o tsv)"
SP_OBJECT_ID="$(az ad sp list --filter "appId eq '$APP_ID'" --query '[0].id' -o tsv)"
if [[ -z "$SP_OBJECT_ID" ]]; then
  SP_OBJECT_ID="$(az ad sp create --id "$APP_ID" --query id -o tsv)"
fi

SCOPE="/subscriptions/${SUBSCRIPTION_ID}"
if ! az role assignment list --assignee-object-id "$SP_OBJECT_ID" --scope "$SCOPE" --query "[?roleDefinitionName=='${ROLE}'] | length(@)" -o tsv | grep -q '^1$'; then
  az role assignment create \
    --assignee-object-id "$SP_OBJECT_ID" \
    --assignee-principal-type ServicePrincipal \
    --role "$ROLE" \
    --scope "$SCOPE" \
    -o none
fi

CREDENTIAL_NAME="github-actions-main"
SUBJECT="repo:${REPO_OWNER}@${REPO_OWNER_ID}/${REPO_NAME}@${REPO_ID}:environment:azure-lab"

EXISTING_ID="$(az ad app federated-credential list \
  --id "$APP_OBJECT_ID" \
  --query "[?name=='${CREDENTIAL_NAME}'].id | [0]" \
  -o tsv)"
EXISTING_SUBJECT="$(az ad app federated-credential list \
  --id "$APP_OBJECT_ID" \
  --query "[?name=='${CREDENTIAL_NAME}'].subject | [0]" \
  -o tsv)"

if [[ -n "$EXISTING_ID" && "$EXISTING_SUBJECT" != "$SUBJECT" ]]; then
  echo "Replacing old federated credential subject:"
  echo "  old: $EXISTING_SUBJECT"
  echo "  new: $SUBJECT"
  az ad app federated-credential delete \
    --id "$APP_OBJECT_ID" \
    --federated-credential-id "$EXISTING_ID"
  EXISTING_ID=""
fi

if [[ -z "$EXISTING_ID" ]]; then
  cat > /tmp/github-oidc.json <<EOF
{
  "name": "${CREDENTIAL_NAME}",
  "issuer": "https://token.actions.githubusercontent.com",
  "subject": "${SUBJECT}",
  "description": "GitHub Actions deployment for ${REPO}",
  "audiences": ["api://AzureADTokenExchange"]
}
EOF
  az ad app federated-credential create --id "$APP_OBJECT_ID" --parameters /tmp/github-oidc.json -o none
fi

cat <<EOF

Azure OIDC trust is ready.

Add/confirm these THREE GitHub repository secrets:

AZURE_CLIENT_ID=${APP_ID}
AZURE_TENANT_ID=${TENANT_ID}
AZURE_SUBSCRIPTION_ID=${SUBSCRIPTION_ID}

GitHub path:
Settings -> Secrets and variables -> Actions

Then run:
Actions -> Deploy Azure Lab -> Run workflow

OIDC subject configured:
${SUBJECT}

No Azure client secret is required.
EOF
