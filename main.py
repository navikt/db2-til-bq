#!/usr/bin/env python3
import os
import shutil
from pathlib import Path
from src.bigquery_connector import BQConnector
from src.db2_connector import DB2Connector

from src.functions import get_from_datetime
from src.class_table import Table, TableType


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


def check_envs():
    # Hvis den ikke finner variabler i miljøet
    # prøv å laster fra .env
    pass


print(f"utvikler lokalt: {local}")

# Copy DB2 license when not running locally
if not local:
    copy_db2_license()


def db2_to_bq(table: Table, bq_client: BQConnector, db2_conn: DB2Connector):
    print(f"-----{table.name}-----")
    print(f"{table.table_type.value} tabell {table.name} ")

    write_disposition = "WRITE_TRUNCATE"

    if table.table_type == TableType.DIM:
        query = table.build_sql_db2()
        df = db2_conn.get_rows_as_dataframe(query=query)

    elif table.table_type == TableType.FAK:

        table_exists_in_bq = bq_client.check_if_table_exists_in_bq(
            table_id=table.bq_table_id
        )
        from_datetime = get_from_datetime(
            bq_client=bq_client, table=table, table_exists_in_bq=table_exists_in_bq
        )
        query = table.build_sql_db2()
        binds = table.generate_binds(from_datetime=from_datetime)
        df = db2_conn.get_rows_as_dataframe(query=query, binds=binds)

        if table_exists_in_bq:
            write_disposition = "WRITE_APPEND"

    else:
        raise ValueError(
            f"Ukjent table_type {table.table_type} for tabell {table.name}."
        )

    if len(df) > 0:
        df.columns = df.columns.str.lower()
        job_config = table.make_bq_load_job_config(write_disposition=write_disposition)
        bq_client.put_dataframe(df, table_id=table.bq_table_id, job_config=job_config)

        print(f"{len(df)} rows written to {table.name}")

    else:
        print(f"Ingen nye rader å laste for tabell {table.name}")


def main():
    from src.config_tables import tables

    if not os.environ.get("GOOGLE_CLOUD_PROJECT"):
        os.environ["GOOGLE_CLOUD_PROJECT"] = "utsikt-dev-3609"  # bør flyttes til .env

    bq_client = BQConnector()
    db2_conn = DB2Connector(
        database_name=os.environ["DATABASE_NAME"],
        username=os.environ["DATABASE_USERNAME"],
        password=os.environ["DATABASE_PASSWORD"],
        port=os.environ["DATABASE_PORT"],
        host=os.environ["DATABASE_HOST"],
    )

    for table in tables:
        db2_to_bq(table, bq_client, db2_conn)


def update_desc():
    from src.config_tables import tables

    bq_client = BQConnector()
    for table in tables:
        bq_client.update_table_and_col_descriptions(
            table_id=table.bq_table_id, desc=table.description, schema=table.cols
        )


if __name__ == "__main__":
    main()
    # update_desc() # Kjøres for å oppdatere tabell og kolonnekommentarer
