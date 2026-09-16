#!/usr/bin/env bash
set -euo pipefail

REPO_OWNER="Muthukumar123"
REPO_OWNER_ID="3875126"
REPO_NAME="apache-pulsar-e2e-PoC"
REPO_ID="1372062775"
ENVIRONMENT_NAME="azure-lab"
APP_NAME="${AZURE_APP_NAME:-github-${REPO_NAME}-deploy}"
CREDENTIAL_NAME="github-actions-immutable"
SUBJECT="repo:${REPO_OWNER}@${REPO_OWNER_ID}/${REPO_NAME}@${REPO_ID}:environment:${ENVIRONMENT_NAME}"

command -v az >/dev/null || { echo "Azure CLI (az) is required"; exit 1; }
az account show >/dev/null 2>&1 || { echo "Azure login required"; exit 1; }

SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
TENANT_ID="$(az account show --query tenantId -o tsv)"

APP_ID="$(az ad app list --display-name "$APP_NAME" --query '[0].appId' -o tsv)"
if [[ -z "$APP_ID" ]]; then
  echo "No Entra application named '$APP_NAME' exists. Run azure/setup-github-oidc.sh first."
  exit 1
fi
APP_OBJECT_ID="$(az ad app show --id "$APP_ID" --query id -o tsv)"

EXACT_COUNT="$(az ad app federated-credential list \
  --id "$APP_OBJECT_ID" \
  --query "[?issuer=='https://token.actions.githubusercontent.com' && subject=='${SUBJECT}'] | length(@)" \
  -o tsv)"

if [[ "$EXACT_COUNT" == "0" ]]; then
  cat > /tmp/github-oidc-immutable.json <<EOF
{
  "name": "${CREDENTIAL_NAME}",
  "issuer": "https://token.actions.githubusercontent.com",
  "subject": "${SUBJECT}",
  "description": "Immutable GitHub Actions OIDC trust for ${REPO_OWNER}/${REPO_NAME}",
  "audiences": ["api://AzureADTokenExchange"]
}
EOF

  # If a credential with this repair name already exists but has a different subject, remove it first.
  OLD_ID="$(az ad app federated-credential list --id "$APP_OBJECT_ID" --query "[?name=='${CREDENTIAL_NAME}'].id | [0]" -o tsv)"
  if [[ -n "$OLD_ID" ]]; then
    az ad app federated-credential delete --id "$APP_OBJECT_ID" --federated-credential-id "$OLD_ID"
  fi

  az ad app federated-credential create \
    --id "$APP_OBJECT_ID" \
    --parameters /tmp/github-oidc-immutable.json \
    -o none
fi

echo
echo "Federated credentials on app $APP_ID:"
az ad app federated-credential list \
  --id "$APP_OBJECT_ID" \
  --query "[].{name:name,issuer:issuer,subject:subject,audiences:audiences}" \
  -o table

echo
VERIFY_COUNT="$(az ad app federated-credential list \
  --id "$APP_OBJECT_ID" \
  --query "[?issuer=='https://token.actions.githubusercontent.com' && subject=='${SUBJECT}' && contains(audiences, 'api://AzureADTokenExchange')] | length(@)" \
  -o tsv)"

if [[ "$VERIFY_COUNT" == "0" ]]; then
  echo "ERROR: exact immutable GitHub OIDC credential was not found after repair."
  exit 1
fi

echo "OIDC credential verified."
echo
echo "IMPORTANT: make sure GitHub Actions secrets contain THESE exact values:"
echo "AZURE_CLIENT_ID=${APP_ID}"
echo "AZURE_TENANT_ID=${TENANT_ID}"
echo "AZURE_SUBSCRIPTION_ID=${SUBSCRIPTION_ID}"
echo
echo "Expected GitHub OIDC subject:"
echo "$SUBJECT"
