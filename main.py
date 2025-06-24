#!/usr/bin/env python3

import os
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
# from google.cloud import bigquery
# import ibm_db
# import ibm_db_sa
from sqlalchemy import create_engine


def read_from_db2(table_name):
    database_username = os.environ.get("DATABASE_USERNAME")
    database_password = os.environ.get("DATABASE_PASSWORD")
    database_host = os.environ.get("DATABASE_HOST", default="155.55.1.82")
    database_port = os.environ.get("DATABASE_PORT", default="5025")
    database_name = os.environ.get("DATABASE_NAME", default="QDB2")

    # Establish the connection
    db2_connection_string = f"db2+ibm_db://{database_username}:{database_password}@{database_host}:{database_port}/{database_name}"

    engine = create_engine(db2_connection_string)
    print(f"Engine created: {engine=}")
    try:
        connection = engine.connect()
    except Exception as e:
        print(f"Unable to instantiate DB2 connection:\n{e}")
        return []
    print(f"Connection established: {connection=}")

    #run query
    schema = os.environ.get("DATABASE_SCHEMA", default="OS231Q1")
    sql = f"select * from {schema}.{table_name}"

    print("Starting sql read/extraction")
    return pd.read_sql(sql, connection)

def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_credentials = service_account.Credentials.from_service_account_file('/var/run/secrets/key')
    storage_client = storage.Client(project='utsikt-dev-3609', credentials=storage_credentials)
    bucket = storage_client.bucket(bucket_name)
    print(list(bucket.list_blobs()))
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)


def main():
    print("Starter jobb")
    filen_finnes = os.path.isfile("/usr/local/lib/python3.12/site-packages/clidriver/license/db2consv_zs.lic")
    print(f"Ligger lisens der forventet?: {filen_finnes=}")

    #legge lisensen et sted
    #download_blob('lisens-db2_utsikt-dev-3609', 'db2consv_zs.lic', 'db2consv_zs.lic')

    print("lese inn data fra db2")
    df = read_from_db2(table_name = 't_faggruppe')
    print(f"hentet {len(df)} rader")


if __name__ == '__main__':
    main()
