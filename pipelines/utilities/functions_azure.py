from azure.mgmt.resource.resources import ResourceManagementClient
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobClient, BlobServiceClient
from azure.identity import DefaultAzureCredential, AzureCliCredential
import sys

try:
    from variables_dev.variables_main import *
except ImportError:
    # TODO: Fix this, since the concept of silently defaulting to Dev on failure to lookup prod variables is not a good one
    from variables_dev.variables_main import (
        subscription_id,keyvault_url,storageaccount_url, storageaccount_container_name,storageaccount_access_key
    )


# Connect Virtual Machine Managed Identity
def connect_az(subscription_id, credential):
    """Connect to Azure Subscription
    
    Parameters:
    - subscription_id (string): ID of the Azure Subscription
    - credential (object): The credentials to be used for authentication.
    
    Returns:
    - Client
    """
    client = ResourceManagementClient(credential, subscription_id=subscription_id)
    return client

# Get Azure credential. AzureCliCredential requires running 'az login' in cmd
def get_az_credential():
    """Obtains an Azure credential, based on the environment you run from (Linux, Windows (local vs DSVM))
    
    Parameters:
    None
    
    Returns:
    - Credential: Credential for connecting to Azure
    """
    if sys.platform.startswith('linux'):
        credential = DefaultAzureCredential()
    elif sys.platform.startswith('win') and 'DeveloperCDH' in sys.path[0] :
        credential = DefaultAzureCredential()
    else:
        credential = AzureCliCredential()
    return credential

# Connect to keyvault and fetch secret
def get_keyvault_secret(keyvault_url,secret_name, credential):
    """Get a secret from the keyvault
    
    Parameters:
    - keyvault_url (string): The URL of the storage account.
    - secret_name (string): The name of the container in the storage account.
    - credential (object): The credentials to be used for authentication.
    
    Returns:
    - String containing the secret
    """
    client = SecretClient(vault_url=keyvault_url, credential=credential)
    return client.get_secret(secret_name)


# Connect to a blob container
def connect_container(storageaccount_url, storageaccount_container_name, storageaccount_access_key, credential):
    """Connect to a Blob container
    
    Parameters:
    - storageaccount_url (string): The URL of the storage account.
    - storageaccount_container_name (string): The name of the container in the storage account.
    - storageaccount_access_key (string): The access key for the storage account.
    - credential (object): The credentials to be used for authentication.
    
    Returns:
    - Container client
    """
    secret = get_keyvault_secret(keyvault_url, storageaccount_access_key, credential)
    blob_service = BlobServiceClient(
        account_url=storageaccount_url, credential=secret.value
    )
    # Connect to container
    return blob_service.get_container_client(storageaccount_container_name)


# Download File as stream from blob
def download_blob_file_to_stream(storageaccount_url,storageaccount_container_name, storageaccount_access_key,filename, credential):
    """Download a blob file to a stream.
    
    Parameters:
    - storageaccount_url (string): The URL of the storage account.
    - storageaccount_container_name (string): The name of the container in the storage account.
    - storageaccount_access_key (string): The access key for the storage account.
    - filename (string): The name of the file to be downloaded.
    - credential (object): The credentials to be used for authentication.
    
    Returns:
    - object: A stream containing the contents of the file.
    """
    container_client = connect_container(storageaccount_url,storageaccount_container_name, storageaccount_access_key, credential)
    blob_client = container_client.get_blob_client(filename)
    return blob_client.download_blob()

# Download File to local path
def download_blob_file(storageaccount_container_name, filename,download_path, credential):
    """
    Downloads a file from specified blob location and writes to a local path 

    Parameters:
    storageaccount_container_name (str): The name of the storage account container where the blob file is located.
    file_path (str): The relative file path within datalake of the file to be downloaded, Eg. 'Raw/Sources/XLS/OPS/In/Project Overview.xlsm'
    download_path (str): The local file path where the blob file will be saved.
    credential (DefaultAzureCredential): Credential to connect to Azure and access the container.
    
    Returns: -
    """
    container_client = connect_container(storageaccount_url, storageaccount_container_name, storageaccount_access_key, credential)
    blob_client = container_client.get_blob_client(filename)
    
    with open(download_path, "wb") as local_file:
            blob_client.download_blob().readinto(local_file)
    print(download_path)

# Upload to Blob
def upload_object_to_blob(data_object, blob_url,storageaccount_access_key, credential):
    """
    Uploads an object to an Azure Blob location

    Parameters:
    - data_object(string): Data object to upload
    - storageaccount_access_key (string): Access key to the storage account
    - storageaccount_access_key (string): The access key for the storage account.
    - credential (DefaultAzureCredential): Credential to connect to Azure

    Returns: -
    """
    secret = get_keyvault_secret(keyvault_url, storageaccount_access_key, credential)
    upload_client = BlobClient.from_blob_url(blob_url=blob_url, credential=secret.value)
    upload_client.upload_blob(data=data_object, overwrite=True)


# Upload dataframe to Blob
def upload_df_to_blob(dataframe, blob_url,storageaccount_access_key, credential, separator=","):
    """
    Converts the dataframe to a data object that will be upload to the blob as csv file
    
    Parameters:
    - dataframe (pandas dataframe): Data object to upload
    - storageaccount_access_key (string): Access key to the storage account
    - storageaccount_access_key (string): The access key for the storage account.
    - credential (DefaultAzureCredential): Credential to connect to Azure
    - separator (string): Seperator to use when writing to CSV, by default ","

    Returns: -
    """

    output = dataframe.to_csv(index=False, encoding="utf-8",sep=separator)
    upload_object_to_blob(output, blob_url,storageaccount_access_key, credential)

