#!/usr/bin/env python3
from src.functions import get_maxval_tgt, read_from_db2, write_to_bigquery
from src.config_tables import tables

local=False # HUSK Å ENDRE FØR PUSH
full_load = False # trenger vi denne?

def main():
    for table in tables[0:3]:
        print(f"Tabell {table.name}:")
        if table.check_col and not full_load: #deltalast
            maxval_tgt = get_maxval_tgt(table = table, local_dev=local)
            df = read_from_db2(db_table=table, local_dev=local, maxval_tgt=maxval_tgt)
        else: #full last
            df = read_from_db2(db_table=table, local_dev=local)
        
        write_disposition = "WRITE_TRUNCATE" if table.check_col else "WRITE_APPEND"
        if len(df)>0:
            write_to_bigquery(df, table_name=table.name, write_disposition=write_disposition, local_dev=local)


if __name__ == '__main__':
    main()
