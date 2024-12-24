# Pandas, azure-storage-blob, dotenv

import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

# data Loading
def run_loading():
    # Loading the dataset
    data = pd.read_csv('cleaneddata.csv')
    products = pd.read_csv('products.csv')
    customers = pd.read_csv('customers.csv')
    staffs = pd.read_csv('staff.csv')
    transactions = pd.read_csv('transaction.csv')

    # Load the environment variables from the .env files
    load_dotenv()

    connect_str = os.getenv('AZURE_CONNECTION_STRING_VALUE')
    container_name = os.getenv('CONTAINER_NAME')

    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Load data to Azure Blob Storage
    files = [
        (data, 'rawdata/cleaned_zipco_transaction_data.csv'),
        (products, 'cleaneddata/products.csv'),
        (customers, 'cleaneddata/customers.csv'),
        (staffs, 'cleaneddata/staff.csv'),
        (transactions, 'cleaneddata/transaction.csv')
    ]

    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f'{blob_name} loaded into Azure Blob Storage')