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

module acrModule './acr.bicep' = {
  name: 'acr-${acrName}'
  scope: rg
  params: {
    location: location
    acrName: acrName
  }
}

output resourceGroupName string = rg.name
output acrName string = acrModule.outputs.acrName
output acrLoginServer string = acrModule.outputs.acrLoginServer
