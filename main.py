#!/usr/bin/env python3
from src.functions import read_from_db2, file_to_bq
from src.config_tables import tables

local=True # HUSK Å ENDRE FØR PUSH



def main():
    for table in tables:
        # finne max_dato eller max beregningid
        print(f"Lese inn data fra db2 for {table.name}:")
        df = read_from_db2(db_table=table, local_dev=local)
        print(f"Hentet {len(df)} rader fra db2")
        print("Skriver til BQ")
        file_to_bq(df, table_name=table.name, local_dev=local)


if __name__ == '__main__':
    main()
