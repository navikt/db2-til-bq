import os
import ibm_db
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account

from src.config_tables import Table

def read_from_db2(db_table: Table, local_dev=False):

    if local_dev:
        from dotenv import load_dotenv
        load_dotenv()

    database_username = os.environ.get("DATABASE_USERNAME")
    database_password = os.environ.get("DATABASE_PASSWORD")
    database_host = os.environ.get("DATABASE_HOST", default="155.55.1.82")
    database_port = os.environ.get("DATABASE_PORT", default="5025")
    database_name = os.environ.get("DATABASE_NAME", default="QDB2")
    schema = os.environ.get("DATABASE_SCHEMA", default="OS231Q2")
    
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

    stmt = ibm_db.exec_immediate(db2_conn, db_table.build_sql(schema=schema))
    rows = []
    row = ibm_db.fetch_assoc(stmt)
    while row:
        rows.append(row)
        row = ibm_db.fetch_assoc(stmt)

    return pd.DataFrame(rows)

def file_to_bq(df, table_name = 't_faggruppe', local_dev=False):
    #write to BQ from df
    if local_dev:
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

if __name__ == "__main__":
    from config_tables import tables

    read_from_db2(db_table=tables[0], local_dev=True)