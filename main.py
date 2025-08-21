#!/usr/bin/env python3

import os
import pandas as pd
import ibm_db
from google.cloud import bigquery
from google.oauth2 import service_account

local=False

def read_from_db2(table_name):
    if local:
        from dotenv import load_dotenv   #for python-dotenv method 
        load_dotenv()

    database_username = os.environ.get("DATABASE_USERNAME")
    database_password = os.environ.get("DATABASE_PASSWORD")
    database_host = os.environ.get("DATABASE_HOST", default="155.55.1.82")
    database_port = os.environ.get("DATABASE_PORT", default="5025")
    database_name = os.environ.get("DATABASE_NAME", default="QDB2")
    
    dsn = (
        f"DRIVER={{IBM DB2 ODBC DRIVER}};"
        f"DATABASE={database_name};"
        f"HOSTNAME={database_host};"
        f"PORT={database_port};"
        f"PROTOCOL=TCPIP;"
        f"UID={database_username};"
        f"PWD={database_password};"
    )
    # Establish the connection
    print("Attempting to connect to database.")
    try:
        db2_conn = ibm_db.connect(dsn, "", "")
        print("Connected to the database!")
    except Exception as e:
        print(e)
        print("Failed to connect to the database.")
        exit(1)

    schema = os.environ.get("DATABASE_SCHEMA", default="OS231Q1")
    sql = f"select * from {schema}.{table_name}"
    stmt = ibm_db.exec_immediate(db2_conn, sql)
    rows = []
    row = ibm_db.fetch_assoc(stmt)
    while row:
        rows.append(row)
        row = ibm_db.fetch_assoc(stmt)

    return pd.DataFrame(rows)

def file_to_bq(df, table_name = 't_faggruppe'):
    #write to BQ from df
    if local:
        bq_client = bigquery.Client(project='utsikt-dev-3609')
    else:
        credentials = service_account.Credentials.from_service_account_file('/var/run/secrets/sa_key.json')
        bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    DATASET='OS231Q2_kopi'

    table_id = DATASET+'.'+table_name

    job_config = bigquery.LoadJobConfig(
        autodetect = True,
        write_disposition = "WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    
    job.result()

def main():
    print("Lese inn data fra db2")
    df = read_from_db2(table_name='t_faggruppe')
    print(f"Hentet {len(df)} rader fra db2")
    print("Skriver til BQ")
    file_to_bq(df, table_name='t_faggruppe')


if __name__ == '__main__':
    main()
