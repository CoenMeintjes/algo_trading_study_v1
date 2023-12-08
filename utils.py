from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

credential = DefaultAzureCredential()

# Azure related
def get_secret(secret_name: str):

    client = SecretClient(vault_url="https://algo1vault.vault.azure.net/", credential=credential)

    # Retrieve the secret by its name
    secret = (client.get_secret(secret_name)).value

    # Get the value of the secret
    return secret
