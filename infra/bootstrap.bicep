targetScope = 'subscription'

@description('Azure region used for the resource group metadata. Existing resource groups keep their original location.')
param resourceGroupLocation string = 'westeurope'

@description('Azure region for deployable lab resources such as ACR.')
param resourceLocation string = 'northeurope'

@description('Resource group containing the complete behavior lab.')
param resourceGroupName string = 'pulsar-eventhub-lab-rg'

@description('Globally unique Azure Container Registry name.')
param acrName string

resource rg 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: resourceGroupName
  location: resourceGroupLocation
}

module acrModule './acr.bicep' = {
  name: 'acr-${acrName}'
  scope: rg
  params: {
    location: resourceLocation
    acrName: acrName
  }
}

output resourceGroupName string = rg.name
output acrName string = acrModule.outputs.acrName
output acrLoginServer string = acrModule.outputs.acrLoginServer
