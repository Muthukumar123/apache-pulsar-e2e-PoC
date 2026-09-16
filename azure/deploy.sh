#!/usr/bin/env bash
set -euo pipefail

LOCATION="${LOCATION:-westeurope}"
RESOURCE_GROUP="${RESOURCE_GROUP:-pulsar-poc-rg}"
ENVIRONMENT="${ENVIRONMENT:-pulsar-poc-env}"
SUFFIX="${PULSAR_DEMO_SUFFIX:-$RANDOM}"
ACR_NAME="${ACR_NAME:-pulsarpoc${SUFFIX}}"
BROKER_APP="${BROKER_APP:-pulsar-broker}"
PRODUCER_APP="${PRODUCER_APP:-pulsar-producer}"
CONSUMER1_APP="${CONSUMER1_APP:-pulsar-consumer-1}"
CONSUMER2_APP="${CONSUMER2_APP:-pulsar-consumer-2}"
CONSUMER3_APP="${CONSUMER3_APP:-pulsar-consumer-3}"

command -v az >/dev/null || { echo "Azure CLI (az) is required"; exit 1; }
az account show >/dev/null 2>&1 || { echo "Run: az login"; exit 1; }

az extension add --name containerapp --upgrade >/dev/null
az provider register --namespace Microsoft.App >/dev/null
az provider register --namespace Microsoft.OperationalInsights >/dev/null

printf '\nCreating Azure resources...\n'
az group create --name "$RESOURCE_GROUP" --location "$LOCATION" -o none
az acr create --name "$ACR_NAME" --resource-group "$RESOURCE_GROUP" --sku Basic --admin-enabled true -o none
az containerapp env create --name "$ENVIRONMENT" --resource-group "$RESOURCE_GROUP" --location "$LOCATION" -o none

printf '\nBuilding producer and consumer images in ACR...\n'
az acr build --registry "$ACR_NAME" --image pulsar-producer:latest --file Dockerfile.producer .
az acr build --registry "$ACR_NAME" --image pulsar-consumer:latest --file Dockerfile.consumer .

REGISTRY_SERVER="${ACR_NAME}.azurecr.io"
REGISTRY_USER="$(az acr credential show -n "$ACR_NAME" --query username -o tsv)"
REGISTRY_PASSWORD="$(az acr credential show -n "$ACR_NAME" --query 'passwords[0].value' -o tsv)"

printf '\nDeploying internal Pulsar broker...\n'
az containerapp create \
  --name "$BROKER_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --environment "$ENVIRONMENT" \
  --image apachepulsar/pulsar:4.2.4 \
  --ingress internal \
  --transport tcp \
  --target-port 6650 \
  --exposed-port 6650 \
  --min-replicas 1 \
  --max-replicas 1 \
  --cpu 1.0 \
  --memory 2Gi \
  --command bin/pulsar standalone \
  --env-vars 'PULSAR_MEM=-Xms512m -Xmx512m -XX:MaxDirectMemorySize=512m' \
  -o none

PULSAR_URL="pulsar://${BROKER_APP}:6650"

printf '\nDeploying Producer UI...\n'
az containerapp create \
  --name "$PRODUCER_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --environment "$ENVIRONMENT" \
  --image "${REGISTRY_SERVER}/pulsar-producer:latest" \
  --registry-server "$REGISTRY_SERVER" \
  --registry-username "$REGISTRY_USER" \
  --registry-password "$REGISTRY_PASSWORD" \
  --ingress external \
  --target-port 8000 \
  --min-replicas 1 \
  --max-replicas 1 \
  --env-vars "PULSAR_SERVICE_URL=${PULSAR_URL}" 'PULSAR_TOPIC=persistent://public/default/demo-messages' \
  -o none

create_consumer() {
  local app_name="$1"
  local consumer_id="$2"
  printf '\nDeploying %s...\n' "$consumer_id"
  az containerapp create \
    --name "$app_name" \
    --resource-group "$RESOURCE_GROUP" \
    --environment "$ENVIRONMENT" \
    --image "${REGISTRY_SERVER}/pulsar-consumer:latest" \
    --registry-server "$REGISTRY_SERVER" \
    --registry-username "$REGISTRY_USER" \
    --registry-password "$REGISTRY_PASSWORD" \
    --ingress external \
    --target-port 8000 \
    --min-replicas 1 \
    --max-replicas 1 \
    --env-vars \
      "PULSAR_SERVICE_URL=${PULSAR_URL}" \
      'PULSAR_TOPIC=persistent://public/default/demo-messages' \
      'PULSAR_DLQ_TOPIC=persistent://public/default/demo-messages-dlq' \
      'PULSAR_SUBSCRIPTION=demo-consumers' \
      'PULSAR_DLQ_SUBSCRIPTION=demo-consumers-dlq' \
      "CONSUMER_ID=${consumer_id}" \
    -o none
}

create_consumer "$CONSUMER1_APP" consumer-1
create_consumer "$CONSUMER2_APP" consumer-2
create_consumer "$CONSUMER3_APP" consumer-3

PRODUCER_FQDN="$(az containerapp show -g "$RESOURCE_GROUP" -n "$PRODUCER_APP" --query properties.configuration.ingress.fqdn -o tsv)"
CONSUMER1_FQDN="$(az containerapp show -g "$RESOURCE_GROUP" -n "$CONSUMER1_APP" --query properties.configuration.ingress.fqdn -o tsv)"
CONSUMER2_FQDN="$(az containerapp show -g "$RESOURCE_GROUP" -n "$CONSUMER2_APP" --query properties.configuration.ingress.fqdn -o tsv)"
CONSUMER3_FQDN="$(az containerapp show -g "$RESOURCE_GROUP" -n "$CONSUMER3_APP" --query properties.configuration.ingress.fqdn -o tsv)"

printf '\nDeployment complete.\n'
printf 'Producer UI:        https://%s\n' "$PRODUCER_FQDN"
printf 'Consumer 1:         https://%s\n' "$CONSUMER1_FQDN"
printf 'Consumer 2:         https://%s\n' "$CONSUMER2_FQDN"
printf 'Consumer 3:         https://%s\n' "$CONSUMER3_FQDN"
printf 'Pulsar internal:    %s\n' "$PULSAR_URL"
printf '\nOpen all four browser URLs. Send a batch (for example 30 messages) and watch Shared subscription distribute messages across consumer-1/2/3.\n'
