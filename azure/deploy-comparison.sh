#!/usr/bin/env bash
set -euo pipefail

LOCATION="${LOCATION:-westeurope}"
RESOURCE_GROUP="${RESOURCE_GROUP:-messaging-lab-rg}"
ENVIRONMENT="${ENVIRONMENT:-messaging-lab-env}"
SUFFIX="${LAB_SUFFIX:-$RANDOM}"
ACR_NAME="${ACR_NAME:-messaginglab${SUFFIX}}"
STORAGE_ACCOUNT="${STORAGE_ACCOUNT:-msglab${SUFFIX}}"
EH_NAMESPACE="${EH_NAMESPACE:-messaging-lab-${SUFFIX}}"
EH_NAME="${EH_NAME:-demo-events}"
EH_CONSUMER_GROUP="${EH_CONSUMER_GROUP:-demo-consumers}"
CHECKPOINT_CONTAINER="${CHECKPOINT_CONTAINER:-eventhub-checkpoints}"

command -v az >/dev/null || { echo "Azure CLI (az) is required"; exit 1; }
az account show >/dev/null 2>&1 || { echo "Run: az login"; exit 1; }
az extension add --name containerapp --upgrade >/dev/null

az group create -n "$RESOURCE_GROUP" -l "$LOCATION" -o none
az acr create -n "$ACR_NAME" -g "$RESOURCE_GROUP" --sku Basic --admin-enabled true -o none
az containerapp env create -n "$ENVIRONMENT" -g "$RESOURCE_GROUP" -l "$LOCATION" -o none

# Event Hubs: deliberately use only two partitions so three consumers make the parallelism ceiling visible.
az eventhubs namespace create -g "$RESOURCE_GROUP" -n "$EH_NAMESPACE" -l "$LOCATION" --sku Standard -o none
az eventhubs eventhub create -g "$RESOURCE_GROUP" --namespace-name "$EH_NAMESPACE" -n "$EH_NAME" --partition-count 2 -o none
az eventhubs eventhub consumer-group create -g "$RESOURCE_GROUP" --namespace-name "$EH_NAMESPACE" --eventhub-name "$EH_NAME" -n "$EH_CONSUMER_GROUP" -o none

az storage account create -g "$RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" -l "$LOCATION" --sku Standard_LRS -o none
BLOB_CONNECTION_STRING="$(az storage account show-connection-string -g "$RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" --query connectionString -o tsv)"
az storage container create --name "$CHECKPOINT_CONTAINER" --connection-string "$BLOB_CONNECTION_STRING" -o none
EVENTHUB_CONNECTION_STRING="$(az eventhubs namespace authorization-rule keys list -g "$RESOURCE_GROUP" --namespace-name "$EH_NAMESPACE" -n RootManageSharedAccessKey --query primaryConnectionString -o tsv)"

az acr build --registry "$ACR_NAME" --image pulsar-producer:latest --file Dockerfile.producer .
az acr build --registry "$ACR_NAME" --image pulsar-consumer:latest --file Dockerfile.consumer .
az acr build --registry "$ACR_NAME" --image eventhub-producer:latest --file Dockerfile.eventhub-producer .
az acr build --registry "$ACR_NAME" --image eventhub-consumer:latest --file Dockerfile.eventhub-consumer .

REGISTRY_SERVER="${ACR_NAME}.azurecr.io"
REGISTRY_USER="$(az acr credential show -n "$ACR_NAME" --query username -o tsv)"
REGISTRY_PASSWORD="$(az acr credential show -n "$ACR_NAME" --query 'passwords[0].value' -o tsv)"

# Pulsar broker + visible producer/consumer apps.
az containerapp create -n pulsar-broker -g "$RESOURCE_GROUP" --environment "$ENVIRONMENT" --image apachepulsar/pulsar:4.2.4 --ingress internal --transport tcp --target-port 6650 --exposed-port 6650 --min-replicas 1 --max-replicas 1 --cpu 1 --memory 2Gi --command bin/pulsar standalone --env-vars 'PULSAR_MEM=-Xms512m -Xmx512m -XX:MaxDirectMemorySize=512m' -o none

az containerapp create -n pulsar-producer -g "$RESOURCE_GROUP" --environment "$ENVIRONMENT" --image "$REGISTRY_SERVER/pulsar-producer:latest" --registry-server "$REGISTRY_SERVER" --registry-username "$REGISTRY_USER" --registry-password "$REGISTRY_PASSWORD" --ingress external --target-port 8000 --min-replicas 1 --max-replicas 1 --env-vars 'PULSAR_SERVICE_URL=pulsar://pulsar-broker:6650' 'PULSAR_TOPIC=persistent://public/default/demo-messages' -o none

for i in 1 2 3; do
  az containerapp create -n "pulsar-consumer-$i" -g "$RESOURCE_GROUP" --environment "$ENVIRONMENT" --image "$REGISTRY_SERVER/pulsar-consumer:latest" --registry-server "$REGISTRY_SERVER" --registry-username "$REGISTRY_USER" --registry-password "$REGISTRY_PASSWORD" --ingress external --target-port 8000 --min-replicas 1 --max-replicas 1 --env-vars 'PULSAR_SERVICE_URL=pulsar://pulsar-broker:6650' 'PULSAR_TOPIC=persistent://public/default/demo-messages' 'PULSAR_DLQ_TOPIC=persistent://public/default/demo-messages-dlq' 'PULSAR_SUBSCRIPTION=demo-consumers' "CONSUMER_ID=pulsar-consumer-$i" -o none
done

# Event Hubs producer + three consumers sharing the same consumer group/checkpoint store.
az containerapp create -n eventhub-producer -g "$RESOURCE_GROUP" --environment "$ENVIRONMENT" --image "$REGISTRY_SERVER/eventhub-producer:latest" --registry-server "$REGISTRY_SERVER" --registry-username "$REGISTRY_USER" --registry-password "$REGISTRY_PASSWORD" --ingress external --target-port 8000 --min-replicas 1 --max-replicas 1 --secrets "eh-connection=$EVENTHUB_CONNECTION_STRING" --env-vars 'EVENTHUB_CONNECTION_STRING=secretref:eh-connection' "EVENTHUB_NAME=$EH_NAME" -o none

for i in 1 2 3; do
  az containerapp create -n "eventhub-consumer-$i" -g "$RESOURCE_GROUP" --environment "$ENVIRONMENT" --image "$REGISTRY_SERVER/eventhub-consumer:latest" --registry-server "$REGISTRY_SERVER" --registry-username "$REGISTRY_USER" --registry-password "$REGISTRY_PASSWORD" --ingress external --target-port 8000 --min-replicas 1 --max-replicas 1 --secrets "eh-connection=$EVENTHUB_CONNECTION_STRING" "blob-connection=$BLOB_CONNECTION_STRING" --env-vars 'EVENTHUB_CONNECTION_STRING=secretref:eh-connection' 'BLOB_STORAGE_CONNECTION_STRING=secretref:blob-connection' "EVENTHUB_NAME=$EH_NAME" "EVENTHUB_CONSUMER_GROUP=$EH_CONSUMER_GROUP" "BLOB_CONTAINER_NAME=$CHECKPOINT_CONTAINER" "CONSUMER_ID=eventhub-consumer-$i" -o none
done

url() { az containerapp show -g "$RESOURCE_GROUP" -n "$1" --query properties.configuration.ingress.fqdn -o tsv; }

echo
echo "=== Pulsar: Shared subscription ==="
echo "Producer:   https://$(url pulsar-producer)"
for i in 1 2 3; do echo "Consumer $i: https://$(url pulsar-consumer-$i)"; done

echo
echo "=== Azure Event Hubs: 2 partitions, 3 consumers ==="
echo "Producer:   https://$(url eventhub-producer)"
for i in 1 2 3; do echo "Consumer $i: https://$(url eventhub-consumer-$i)"; done

echo
echo "Experiment: send 30 messages in each producer and compare the three consumer dashboards side by side."
echo "Pulsar: all three consumers can receive individual messages from one Shared subscription."
echo "Event Hubs: only two consumers should own partitions; the third should show owned partitions = none."
