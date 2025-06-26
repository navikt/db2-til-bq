#!/usr/bin/env python3

import os
import pandas as pd
import ibm_db

def read_from_db2_2(table_name):
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
    try:
        ibm_db.debug(True)
        db2_conn = ibm_db.connect(dsn, "", "")
        print("Connected to the database!")
    except:
        print("Failed to connect to the database.")
        while True:
            pass
        exit(1)

    schema = os.environ.get("DATABASE_SCHEMA", default="OS231Q1")
    sql = f"select * from {schema}.{table_name}"
    stmt = ibm_db.exec_immediate(db2_conn, sql)
    rows = []
    row = ibm_db.fetch_assoc(stmt)
    while row:
        rows.append(row)
        row = ibm_db.fetch_assoc(stmt)

    df = pd.DataFrame(rows)

    print(f"hentet {len(df)} rader")
    return(df)


def main():
    print("Starter jobb")
    filen_finnes = os.path.isfile("/usr/local/lib/python3.12/site-packages/clidriver/license/db2consv_zs.lic")
    print(f"Ligger lisens der forventet?: {filen_finnes=}")

    print("lese inn data fra db2")
    df = read_from_db2_2(table_name = 't_faggruppe')
    print(f"hentet {len(df)} rader")


if __name__ == '__main__':
    main()
