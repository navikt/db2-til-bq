#!/usr/bin/env python3
import os


from google.cloud.exceptions import NotFound

from src.functions import (
    get_maxval_tgt,
    read_from_db2,
    create_db2_conn,
)
from src.fetch_chunk import fetching_table
from src.config_tables import tables
from src.class_table import Table
from dotenv import load_dotenv

load_dotenv()


def db2_to_bq(table: Table, db2_conn):

    print(f"Tabell {table.name}")
    fetching_table(conn=db2_conn, db_table=table, chunk_size=100000)


def main():
    db2_conn = create_db2_conn(local_dev=True)
    for table in tables:
        if table.name == "t_vent_stoppnivaa":
            db2_to_bq(table, db2_conn)


if __name__ == "__main__":
    main()
