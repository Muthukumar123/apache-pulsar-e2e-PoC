@description('Azure region.')
param location string = resourceGroup().location

@description('Existing ACR name created by bootstrap.bicep.')
param acrName string

@description('Suffix used to keep globally unique resource names.')
@minLength(6)
param suffix string

param containerAppsEnvironmentName string = 'pulsar-eventhub-lab-env'
param eventHubName string = 'demo-events'
param eventHubConsumerGroup string = 'demo-consumers'
param eventHubPartitionCount int = 2
param checkpointContainerName string = 'eventhub-checkpoints'

var pulsarBrokerName = 'pulsar-broker'
var pulsarProducerName = 'pulsar-producer'
var pulsarConsumerNames = [
  'pulsar-consumer-1'
  'pulsar-consumer-2'
  'pulsar-consumer-3'
]
var eventHubProducerName = 'eventhub-producer'
var eventHubConsumerNames = [
  'eventhub-consumer-1'
  'eventhub-consumer-2'
  'eventhub-consumer-3'
]
var eventHubNamespaceName = 'peh-${suffix}'
var storageAccountName = toLower('peh${suffix}st')

resource acr 'Microsoft.ContainerRegistry/registries@2025-11-01' existing = {
  name: acrName
}

var acrCredentials = acr.listCredentials()
var acrPassword = acrCredentials.passwords[0].value

resource env 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: containerAppsEnvironmentName
  location: location
  properties: {}
}

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: eventHubNamespaceName
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
  properties: {
    isAutoInflateEnabled: false
    disableLocalAuth: false
    publicNetworkAccess: 'Enabled'
    minimumTlsVersion: '1.2'
  }
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: eventHubNamespace
  name: eventHubName
  properties: {
    partitionCount: eventHubPartitionCount
    messageRetentionInDays: 1
  }
}

resource consumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: eventHub
  name: eventHubConsumerGroup
  properties: {}
}

resource eventHubAccess 'Microsoft.EventHub/namespaces/authorizationRules@2024-01-01' = {
  parent: eventHubNamespace
  name: 'lab-send-listen'
  properties: {
    rights: [
      'Listen'
      'Send'
    ]
  }
}

var eventHubConnectionString = eventHubAccess.listKeys().primaryConnectionString

resource storage 'Microsoft.Storage/storageAccounts@2025-01-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    publicNetworkAccess: 'Enabled'
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2025-01-01' = {
  parent: storage
  name: 'default'
}

resource checkpointContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = {
  parent: blobService
  name: checkpointContainerName
  properties: {
    publicAccess: 'None'
  }
}

var storageKey = storage.listKeys().keys[0].value
var blobConnectionString = 'DefaultEndpointsProtocol=https;AccountName=${storage.name};AccountKey=${storageKey};EndpointSuffix=${environment().suffixes.storage}'

resource pulsarBroker 'Microsoft.App/containerApps@2024-03-01' = {
  name: pulsarBrokerName
  location: location
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: false
        transport: 'tcp'
        targetPort: 6650
        exposedPort: 6650
        traffic: [
          {
            latestRevision: true
            weight: 100
          }
        ]
      }
    }
    template: {
      containers: [
        {
          name: 'pulsar'
          image: 'apachepulsar/pulsar:4.2.4'
          command: [
            'bin/pulsar'
          ]
          args: [
            'standalone'
            '--advertised-address'
            pulsarBrokerName
            '--no-functions-worker'
            '--no-stream-storage'
          ]
          env: [
            {
              name: 'PULSAR_MEM'
              value: '-Xms1024m -Xmx1024m -XX:MaxDirectMemorySize=1024m'
            }
          ]
          resources: {
            cpu: json('2.0')
            memory: '4Gi'
          }
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

var pulsarServiceUrl = 'pulsar://${pulsarBrokerName}:6650'
var registryConfig = [
  {
    server: acr.properties.loginServer
    username: acrCredentials.username
    passwordSecretRef: 'acr-password'
  }
]
var registrySecret = [
  {
    name: 'acr-password'
    value: acrPassword
  }
]

resource pulsarProducer 'Microsoft.App/containerApps@2024-03-01' = {
  name: pulsarProducerName
  location: location
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: registrySecret
      registries: registryConfig
      ingress: {
        external: true
        transport: 'auto'
        targetPort: 8000
        allowInsecure: false
        traffic: [
          {
            latestRevision: true
            weight: 100
          }
        ]
      }
    }
    template: {
      containers: [
        {
          name: 'producer'
          image: '${acr.properties.loginServer}/pulsar-producer:latest'
          env: [
            {
              name: 'PULSAR_SERVICE_URL'
              value: pulsarServiceUrl
            }
            {
              name: 'PULSAR_TOPIC'
              value: 'persistent://public/default/demo-messages'
            }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

resource pulsarConsumers 'Microsoft.App/containerApps@2024-03-01' = [for (consumerName, i) in pulsarConsumerNames: {
  name: consumerName
  location: location
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: registrySecret
      registries: registryConfig
      ingress: {
        external: true
        transport: 'auto'
        targetPort: 8000
        allowInsecure: false
        traffic: [
          {
            latestRevision: true
            weight: 100
          }
        ]
      }
    }
    template: {
      containers: [
        {
          name: 'consumer'
          image: '${acr.properties.loginServer}/pulsar-consumer:latest'
          env: [
            {
              name: 'PULSAR_SERVICE_URL'
              value: pulsarServiceUrl
            }
            {
              name: 'PULSAR_TOPIC'
              value: 'persistent://public/default/demo-messages'
            }
            {
              name: 'PULSAR_DLQ_TOPIC'
              value: 'persistent://public/default/demo-messages-dlq'
            }
            {
              name: 'PULSAR_SUBSCRIPTION'
              value: 'demo-consumers'
            }
            {
              name: 'CONSUMER_ID'
              value: 'consumer-${i + 1}'
            }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}]

resource eventHubProducer 'Microsoft.App/containerApps@2024-03-01' = {
  name: eventHubProducerName
  location: location
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: concat(registrySecret, [
        {
          name: 'eventhub-connection'
          value: eventHubConnectionString
        }
      ])
      registries: registryConfig
      ingress: {
        external: true
        transport: 'auto'
        targetPort: 8000
        allowInsecure: false
        traffic: [
          {
            latestRevision: true
            weight: 100
          }
        ]
      }
    }
    template: {
      containers: [
        {
          name: 'producer'
          image: '${acr.properties.loginServer}/eventhub-producer:latest'
          env: [
            {
              name: 'EVENTHUB_CONNECTION_STRING'
              secretRef: 'eventhub-connection'
            }
            {
              name: 'EVENTHUB_NAME'
              value: eventHub.name
            }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

resource eventHubConsumers 'Microsoft.App/containerApps@2024-03-01' = [for (consumerName, i) in eventHubConsumerNames: {
  name: consumerName
  location: location
  properties: {
    managedEnvironmentId: env.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: concat(registrySecret, [
        {
          name: 'eventhub-connection'
          value: eventHubConnectionString
        }
        {
          name: 'blob-connection'
          value: blobConnectionString
        }
      ])
      registries: registryConfig
      ingress: {
        external: true
        transport: 'auto'
        targetPort: 8000
        allowInsecure: false
        traffic: [
          {
            latestRevision: true
            weight: 100
          }
        ]
      }
    }
    template: {
      containers: [
        {
          name: 'consumer'
          image: '${acr.properties.loginServer}/eventhub-consumer:latest'
          env: [
            {
              name: 'EVENTHUB_CONNECTION_STRING'
              secretRef: 'eventhub-connection'
            }
            {
              name: 'EVENTHUB_NAME'
              value: eventHub.name
            }
            {
              name: 'EVENTHUB_CONSUMER_GROUP'
              value: consumerGroup.name
            }
            {
              name: 'BLOB_STORAGE_CONNECTION_STRING'
              secretRef: 'blob-connection'
            }
            {
              name: 'BLOB_CONTAINER_NAME'
              value: checkpointContainer.name
            }
            {
              name: 'CONSUMER_ID'
              value: 'eventhub-consumer-${i + 1}'
            }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}]

output pulsarProducerUrl string = 'https://${pulsarProducer.properties.configuration.ingress.fqdn}'
output pulsarConsumerUrls array = [for i in range(0, length(pulsarConsumerNames)): 'https://${pulsarConsumers[i].properties.configuration.ingress.fqdn}']
output eventHubProducerUrl string = 'https://${eventHubProducer.properties.configuration.ingress.fqdn}'
output eventHubConsumerUrls array = [for i in range(0, length(eventHubConsumerNames)): 'https://${eventHubConsumers[i].properties.configuration.ingress.fqdn}']
output eventHubNamespace string = eventHubNamespace.name
output eventHubPartitions int = eventHubPartitionCount
output resourceGroupName string = resourceGroup().name