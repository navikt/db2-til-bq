import os
import ibm_db
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account

from src.class_table import Table


def create_bq_client(local_dev: bool = False):
    if local_dev:
        bq_client = bigquery.Client(project="utsikt-dev-3609")
    else:
        credentials = service_account.Credentials.from_service_account_file(
            "/var/run/secrets/sa_key.json"
        )
        bq_client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )
    return bq_client


def set_bq_dataset():
    db_schema = os.environ.get("DATABASE_SCHEMA")
    bq_dataset = db_schema[:2] + "_" + db_schema[-2:]
    return bq_dataset


def create_db2_conn(local_dev: bool = False):
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
        exit(1)
    return db2_conn


def get_maxval_tgt(table: Table, bq_client, table_id):

    max_query = f"SELECT MAX({table.check_col}) FROM {table_id}"
    maxval_tgt = bq_client.query(max_query).result().to_dataframe().iloc[0, 0]
    return maxval_tgt


def read_from_db2(db_table: Table, db2_conn, maxval_tgt=None):

    query = db_table.build_sql(
        schema=os.environ.get("DATABASE_SCHEMA"), maxval_tgt=maxval_tgt
    )
    print(query)
    stmt = ibm_db.exec_immediate(
        db2_conn, query
    )  # With the default forward-only cursor, each call to a fetch method returns the next row in the result set.
    rows = []
    row = ibm_db.fetch_assoc(
        stmt
    )  # Returns a dictionary, which is indexed by column name, representing a row in a result set.
    while row:
        rows.append(row)
        row = ibm_db.fetch_assoc(stmt)

    df = pd.DataFrame(rows)
    print(f"Hentet {len(df)} rader fra db2")

    return df


def write_to_bigquery(df, bq_client, table_id, write_disposition: str):
    # write to BQ from df

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=write_disposition,
        create_disposition="CREATE_IF_NEEDED",
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)

    job.result()

    print(
        f"Written {len(df)} rows to table {table_id} using {write_disposition} with output bytes: {job.output_bytes}"
    )


if __name__ == "__main__":
    print("This is a module, not to be run directly")
