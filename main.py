#!/usr/bin/env python3
from src.functions import get_maxval_tgt, read_from_db2, write_to_bigquery
from src.config_tables import tables
from src.class_table import Table

#TODO local, dev og prod config
local=False # HUSK Å ENDRE FØR PUSH

def db2_to_bq(table:Table):
##TODO force method to overrun load_method listed in config_tables.py

    print(f"Tabell {table.name}:")
    if table.load_method == 'delta' : #delta last
        maxval_tgt = get_maxval_tgt(table = table, local_dev=local)
        df = read_from_db2(db_table=table, local_dev=local, maxval_tgt=maxval_tgt)
        write_disposition = "WRITE_APPEND"
    else: #full last
        df = read_from_db2(db_table=table, local_dev=local)
        write_disposition = "WRITE_TRUNCATE"
    
    if len(df)>0:
        write_to_bigquery(df, table_name=table.name, write_disposition=write_disposition, local_dev=local)


def main():
    for table in tables[0:3]:
        db2_to_bq(table)


if __name__ == '__main__':
    main()
