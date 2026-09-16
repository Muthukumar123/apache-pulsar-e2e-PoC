targetScope = 'subscription'

@description('Azure region for the lab.')
param location string = 'westeurope'

@description('Resource group containing the complete behavior lab.')
param resourceGroupName string = 'pulsar-eventhub-lab-rg'

@description('Globally unique Azure Container Registry name.')
param acrName string

resource rg 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: resourceGroupName
  location: location
}

resource acr 'Microsoft.ContainerRegistry/registries@2025-11-01' = {
  scope: rg
  name: acrName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: true
    publicNetworkAccess: 'Enabled'
  }
}

output resourceGroupName string = rg.name
output acrName string = acr.name
output acrLoginServer string = acr.properties.loginServer
