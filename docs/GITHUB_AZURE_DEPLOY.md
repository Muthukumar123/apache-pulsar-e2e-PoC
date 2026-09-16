# Deploy the complete lab from GitHub Actions

After one-time Azure OIDC setup, normal deployment is entirely from GitHub:

1. Open the repository.
2. Open **Actions**.
3. Select **Deploy Azure Lab**.
4. Click **Run workflow**.
5. Keep the default `westeurope` region and `pulsar-eventhub-lab-rg` resource group, or change them.
6. Open the `Provision and deploy lab` job.
7. When it completes, open the **Summary** page. It contains the Pulsar producer/consumer URLs and Event Hubs producer/consumer URLs.

The job performs all of these operations:

- Azure OIDC login
- Bicep validation
- Azure provider registration
- resource group + ACR deployment
- ACR image builds
- Container Apps environment creation
- internal Pulsar broker deployment
- Pulsar producer + 3 consumer dashboards
- Event Hubs namespace + 2-partition Event Hub
- Event Hubs consumer group
- Blob Storage checkpoint store
- Event Hubs producer + 3 consumer dashboards

## One-time Azure OIDC setup

GitHub Actions cannot deploy into an Azure subscription until Azure trusts the repository identity. This is a one-time bootstrap operation.

From Azure Cloud Shell (Bash), clone the repository and run:

```bash
git clone https://github.com/Muthukumar123/apache-pulsar-e2e-PoC.git
cd apache-pulsar-e2e-PoC
chmod +x azure/setup-github-oidc.sh
./azure/setup-github-oidc.sh
```

The script creates/reuses an Entra application, service principal, Contributor role assignment at subscription scope, and a federated credential for the GitHub `azure-lab` environment. It prints three values.

Add those values to **GitHub repository secrets** under:

`Settings -> Secrets and variables -> Actions -> New repository secret`

Create exactly these secrets:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`

No Azure client secret/password is required.

## Normal deployment after setup

No terminal is needed after the bootstrap. Use:

`GitHub -> Actions -> Deploy Azure Lab -> Run workflow`

The workflow file is `.github/workflows/deploy-azure-lab.yml`.

## Cleanup

When you finish the lab, delete the resource group from Azure Portal or run:

```bash
az group delete --name pulsar-eventhub-lab-rg --yes --no-wait
```
