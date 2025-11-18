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
from src.config_tables import static_tables, tables
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


def db2_to_bq(table: Table, bq_client, db2_conn):

    DATASET = set_bq_dataset()
    table_id = DATASET + "." + table.name
    try:
        bq_client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
    except NotFound:
        print("Table {} is not found.".format(table_id))
        table.set_load_method_to_full()
        ##TODO gjøre noe annet i prod for init-last, sette min tidspunkt til 2 år tilbake i tid

    print(f"Tabell {table.name} med load method {table.load_method}")
    if table.load_method == "delta":  # delta last
        maxval_tgt = get_maxval_tgt(table=table, bq_client=bq_client, table_id=table_id)
        df = read_from_db2(db_table=table, db2_conn=db2_conn, maxval_tgt=maxval_tgt)
        write_disposition = "WRITE_APPEND"
    else:  # full last
        df = read_from_db2(db_table=table, db2_conn=db2_conn)
        write_disposition = "WRITE_TRUNCATE"

    if len(df) > 0:
        write_to_bigquery(
            df,
            bq_client=bq_client,
            table_id=table_id,
            write_disposition=write_disposition,
        )


def main():
    bq_client = create_bq_client(local_dev=local)
    db2_conn = create_db2_conn(local_dev=local)
    for table in tables:
        db2_to_bq(table, bq_client, db2_conn)


def run_static_tables():
    bq_client = create_bq_client(local_dev=local)
    db2_conn = create_db2_conn(local_dev=local)
    for table in static_tables:
        db2_to_bq(table, bq_client, db2_conn)


if __name__ == "__main__":
    main()

# TODO lag en testfunksjon slik at man slipper å messe med main
# TODO gjør oppsett som kjører static tables
