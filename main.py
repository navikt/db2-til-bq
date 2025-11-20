#!/usr/bin/env python3
import os
import shutil
from pathlib import Path

from google.cloud.exceptions import NotFound

from src.functions import (
    get_maxval_tgt,
    read_from_db2,
    write_to_bigquery,
    create_bq_client,
    set_bq_dataset,
    create_db2_conn,
)
from src.config_tables import tables
from src.class_table import Table


def copy_db2_license():
    """Copy DB2 license file to the appropriate location if it exists."""
    license_source = Path("/var/run/secrets/db2-license/db2consv_zs.lic")
    license_destination = Path(
        "/app/venv/lib/python3.13/site-packages/clidriver/license/db2consv_zs.lic"
    )

    if license_source.exists():
        try:
            # Resolve any symlinks and copy the actual file
            resolved_source = license_source.resolve()
            shutil.copy2(resolved_source, license_destination)
            print(f"DB2 license copied from {resolved_source} to {license_destination}")
        except Exception as e:
            print(f"Warning: Failed to copy DB2 license file: {e}")
    else:
        print("DB2 license file not found, skipping copy")


if "NAIS_CLUSTER_NAME" in os.environ:
    local = False
else:
    local = True
    from dotenv import load_dotenv

    load_dotenv()

print(f"utvikler lokalt: {local}")

# Copy DB2 license when not running locally
if not local:
    copy_db2_license()


def check_if_table_exists_in_bq(bq_client, table_id):
    # Check if table exists in BQ, and set appropriate load method.
    try:
        bq_client.get_table(table_id)  # Make an API request.
        table_exists_in_bq = True
        print("Tabellen {} finnes i BQ.".format(table_id))
    except NotFound:
        table_exists_in_bq = False
        print("Tabellen {} finnes ikke i BQ.".format(table_id))

    return table_exists_in_bq


def db2_to_bq(table: Table, bq_client, db2_conn):
    print(f"-----{table.name}-----")  # print for å skille tabellene i loggen

    # Set dataset and table id
    DATASET = set_bq_dataset()
    table_id = DATASET + "." + table.name

    if table.table_type == "dim":
        load_method = "full"
        print(f"{table.table_type}tabell {table.name} med {load_method} load method")
        df = read_from_db2(db_table=table, db2_conn=db2_conn, load_method=load_method)
        write_disposition = "WRITE_TRUNCATE"

    elif table.table_type == "fak":

        table_exists_in_bq = check_if_table_exists_in_bq(bq_client, table_id)

        if table_exists_in_bq:
            load_method = "delta"
            print(
                f"{table.table_type}tabell {table.name} med {load_method} load method"
            )
            maxval_tgt = get_maxval_tgt(
                table=table, bq_client=bq_client, table_id=table_id
            )
            df = read_from_db2(
                db_table=table,
                db2_conn=db2_conn,
                load_method=load_method,
                maxval_tgt=maxval_tgt,
            )
            write_disposition = "WRITE_APPEND"
        else:
            load_method = "full"  # gjøre om til en init load istedet, skal ikke hente alle rader for faktatabeller.
            print(
                f"{table.table_type}tabell {table.name} med {load_method} load method"
            )
            df = read_from_db2(
                db_table=table, db2_conn=db2_conn, load_method=load_method
            )
            write_disposition = "WRITE_TRUNCATE"

    else:
        raise ValueError(
            f"Ukjent table_type {table.table_type} for tabell {table.name}."
        )

    if len(df) > 0:
        write_to_bigquery(
            df,
            bq_client=bq_client,
            table_id=table_id,
            table_type=table.table_type,
            write_disposition=write_disposition,
        )
    elif load_method == "delta":
        print(f"Ingen nye rader å laste for tabell {table.name}")
    else:
        print(f"Ingen rader hentet fra kildetabell {table.name}")


def main():
    bq_client = create_bq_client(local_dev=local)
    db2_conn = create_db2_conn(local_dev=local)

    for table in tables:
        db2_to_bq(table, bq_client, db2_conn)


if __name__ == "__main__":
    main()

# TODO lag en testfunksjon slik at man slipper å messe med main
# TODO gjør oppsett som kjører static tables
