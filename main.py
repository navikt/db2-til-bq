#!/usr/bin/env python3
import os 

from google.cloud.exceptions import NotFound

from src.functions import get_maxval_tgt, read_from_db2, write_to_bigquery, create_bq_client, set_bq_dataset, create_db2_conn
from src.config_tables import tables
from src.class_table import Table

if 'NAIS_CLUSTER_NAME' in os.environ:
    local=False 
else:
    local=True
    from dotenv import load_dotenv
    load_dotenv()

print(f"utvikler lokalt: {local}")

def db2_to_bq(table: Table, bq_client, db2_conn):

    DATASET=set_bq_dataset()
    table_id = DATASET+'.'+ table.name
    try:
        bq_client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
    except NotFound:
        print("Table {} is not found.".format(table_id))
        table.set_load_method_to_full()
        ##TODO gjøre noe annet i prod for init-last, sette min tidspunkt til 2 år tilbake i tid

    print(f"Tabell {table.name} med load method {table.load_method}")
    if table.load_method == 'delta' : #delta last
        maxval_tgt = get_maxval_tgt(table = table, bq_client=bq_client, table_id=table_id)
        df = read_from_db2(db_table=table, db2_conn=db2_conn, maxval_tgt=maxval_tgt)
        write_disposition = "WRITE_APPEND"
    else: #full last
        df = read_from_db2(db_table=table, db2_conn=db2_conn)
        write_disposition = "WRITE_TRUNCATE"
    
    if len(df)>0:
        write_to_bigquery(df, bq_client=bq_client, table_id=table_id, write_disposition=write_disposition)


def main():
    bq_client = create_bq_client(local_dev=local)
    db2_conn = create_db2_conn(local_dev=local)
    for table in tables:
        db2_to_bq(table, bq_client, db2_conn)


if __name__ == '__main__':
    main()
