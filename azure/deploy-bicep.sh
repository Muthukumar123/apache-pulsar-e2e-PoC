#!/usr/bin/env bash
set -euo pipefail

LOCATION="${LOCATION:-northeurope}"
RESOURCE_GROUP="${RESOURCE_GROUP:-pulsar-eventhub-lab-rg}"
RAW_SUFFIX="${LAB_SUFFIX:-$RANDOM}"
SUFFIX="$(printf '%06d' "$((10#${RAW_SUFFIX//[^0-9]/} % 1000000))")"
ACR_NAME="${ACR_NAME:-pehlab${SUFFIX}}"
SUBSCRIPTION_ID="${AZURE_SUBSCRIPTION_ID:-}"

command -v az >/dev/null || { echo "Azure CLI (az) is required"; exit 1; }
az account show >/dev/null 2>&1 || { echo "Run: az login"; exit 1; }

if [[ -n "$SUBSCRIPTION_ID" ]]; then
  az account set --subscription "$SUBSCRIPTION_ID"
fi

ACTIVE_SUBSCRIPTION="$(az account show --query id -o tsv)"
ACTIVE_NAME="$(az account show --query name -o tsv)"

if az group show --name "$RESOURCE_GROUP" >/dev/null 2>&1; then
  RESOURCE_GROUP_LOCATION="$(az group show --name "$RESOURCE_GROUP" --query location -o tsv)"
else
  RESOURCE_GROUP_LOCATION="$LOCATION"
fi

echo "Azure subscription:     $ACTIVE_NAME ($ACTIVE_SUBSCRIPTION)"
echo "Lab resource location:  $LOCATION"
echo "Resource group:         $RESOURCE_GROUP"
echo "Resource group location:$RESOURCE_GROUP_LOCATION"
echo "ACR:                    $ACR_NAME"
echo "Lab suffix:             $SUFFIX"

az provider register --namespace Microsoft.App --wait >/dev/null
az provider register --namespace Microsoft.EventHub --wait >/dev/null
az provider register --namespace Microsoft.Storage --wait >/dev/null
az provider register --namespace Microsoft.ContainerRegistry --wait >/dev/null

printf '\n[1/3] Bootstrapping resource group and ACR with Bicep...\n'
az deployment sub create \
  --name "pulsar-eventhub-bootstrap-${SUFFIX}" \
  --location "$RESOURCE_GROUP_LOCATION" \
  --template-file infra/bootstrap.bicep \
  --parameters resourceGroupLocation="$RESOURCE_GROUP_LOCATION" resourceLocation="$LOCATION" resourceGroupName="$RESOURCE_GROUP" acrName="$ACR_NAME" \
  --only-show-errors \
  -o none

printf '\n[2/3] Building application images in ACR...\n'
az acr build --registry "$ACR_NAME" --image pulsar-producer:latest --file Dockerfile.producer . --only-show-errors
az acr build --registry "$ACR_NAME" --image pulsar-consumer:latest --file Dockerfile.consumer . --only-show-errors
az acr build --registry "$ACR_NAME" --image eventhub-producer:latest --file Dockerfile.eventhub-producer . --only-show-errors
az acr build --registry "$ACR_NAME" --image eventhub-consumer:latest --file Dockerfile.eventhub-consumer . --only-show-errors

printf '\n[3/3] Deploying Pulsar + Event Hubs behavior lab with Bicep...\n'
az deployment group create \
  --name "pulsar-eventhub-lab-${SUFFIX}" \
  --resource-group "$RESOURCE_GROUP" \
  --template-file infra/main.bicep \
  --parameters location="$LOCATION" acrName="$ACR_NAME" suffix="$SUFFIX" \
  --only-show-errors \
  -o none

OUTPUTS="$(az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --name "pulsar-eventhub-lab-${SUFFIX}" \
  --query properties.outputs \
  -o json)"

python3 - "$OUTPUTS" <<'PY'
import json, sys
x = json.loads(sys.argv[1])

def value(name):
    return x[name]["value"]

print("\nDeployment complete.\n")
print("PULSAR")
print("  Producer:   ", value("pulsarProducerUrl"))
for i, url in enumerate(value("pulsarConsumerUrls"), 1):
    print(f"  Consumer {i}:  {url}")

print("\nAZURE EVENT HUBS")
print("  Producer:   ", value("eventHubProducerUrl"))
for i, url in enumerate(value("eventHubConsumerUrls"), 1):
    print(f"  Consumer {i}:  {url}")
print("  Namespace:  ", value("eventHubNamespace"))
print("  Partitions: ", value("eventHubPartitions"))

print("\nOpen all dashboards side by side and send the same burst through each producer.")
PY

printf '\nCleanup when finished:\n'
printf 'az group delete --name %q --yes --no-wait\n' "$RESOURCE_GROUP"
