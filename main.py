
import os
import pandas as pd

from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
import ibm_db
import ibm_db_sa
from sqlalchemy import create_engine



def read_from_db2(table_name):

    database_username = os.environ.get("DATABASE_USERNAME")
    database_password = os.environ.get("DATABASE_PASSWORD")
    database_host = os.environ.get("DATABASE_HOST")
    database_port = os.environ.get("DATABASE_PORT")
    database_name = os.environ.get("DATABASE_NAME")
    schema = os.environ.get("DATABASE_SCHEMA")


    # Establish the connection
    db2_connection_string = f"db2+ibm_db://{database_username}:{database_password}@{database_host}:{database_port}/{database_name}"

    engine = create_engine(db2_connection_string)
    connection = engine.connect() 

    #run query
    sql = f"select * from {schema}.{table_name}"

    df = pd.read_sql(sql, connection)

def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_credentials = service_account.Credentials.from_service_account_file('/var/run/secrets/key')
    storage_client = storage.Client(project='utsikt-dev-3609', credentials=storage_credentials)
    bucket = storage_client.bucket(bucket_name)
    print(list(bucket.list_blobs()))
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)



def main():

    #legge lisensen et sted
    #download_blob('lisens-db2_utsikt-dev-3609', 'db2consv_zs.lic', 'db2consv_zs.lic')

    #lese inn data fra db2
    df = read_from_db2(table_name = 't_faggruppe')
    print(f"hentet {len(df)} rader")


if __name__ == '__main__':
    main()
