#!/usr/bin/env python3
import os 

from google.cloud.exceptions import NotFound

from src.functions import get_maxval_tgt, read_from_db2, write_to_bigquery, _create_bq_client, _set_bq_dataset
from src.config_tables import tables
from src.class_table import Table

if 'NAIS_CLUSTER_NAME' in os.environ:
    local=False 
else:
    local=True
    from dotenv import load_dotenv
    load_dotenv()

print(f"utvikler lokalt: {local}")

def db2_to_bq(table: Table, bq_client):

    DATASET=_set_bq_dataset()
    table_id = DATASET+'.'+ table.name
    try:
        bq_client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
    except NotFound:
        print("Table {} is not found.".format(table_id))
        table.set_load_method_to_full()
        ##TODO gjøre noe annet i prod, sette min tidspunkt til 2 år tilbake i tid

    print(f"Tabell {table.name} med load method {table.load_method}")
    if table.load_method == 'delta' : #delta last
        maxval_tgt = get_maxval_tgt(table = table, bq_client=bq_client)
        df = read_from_db2(db_table=table, local_dev=local, maxval_tgt=maxval_tgt)
        write_disposition = "WRITE_APPEND"
    else: #full last
        df = read_from_db2(db_table=table, local_dev=local)
        write_disposition = "WRITE_TRUNCATE"
    
    if len(df)>0:
        write_to_bigquery(df, table_name=table.name, write_disposition=write_disposition, local_dev=local)


def main():
    bq_client = _create_bq_client(local_dev=local)
    #create db2_client here too
    for table in tables:
        db2_to_bq(table, bq_client)


if __name__ == '__main__':
    main()
