#!/usr/bin/env python3
import os
import shutil
from pathlib import Path
from src.bigquery_connector import BQConnector
from src.db2_connector import DB2Connector

from src.functions import get_from_date, set_bq_dataset
from src.config_tables import tables
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


def db2_to_bq(table: Table, bq_client, db2_conn):
    print(f"-----{table.name}-----")  # print for å skille tabellene i loggen

    # Set dataset and table id
    DATASET = set_bq_dataset()
    table_id = DATASET + "." + table.name

    if table.table_type == TableType.DIM:
        load_method = "full"
        print(f"{table.table_type}tabell {table.name} med {load_method} load method")
        # df = read_from_db2(db_table=table, db2_conn=db2_conn, load_method=load_method)
        query = table.build_sql(
            schema=os.environ.get("DATABASE_SCHEMA"), load_method=load_method
        )
        binds = {}
        df = db2_conn.get_rows_as_dataframe(query=query, binds=binds)

        write_disposition = "WRITE_TRUNCATE"

    elif table.table_type == TableType.FAK:
        load_method = "delta"
        table_id = f"{set_bq_dataset()}.{table.name}"

        table_exists_in_bq = bq_client.check_if_table_exists_in_bq(table_id)
        date_from = get_from_date(bq_client, table, table_id, table_exists_in_bq)

        query = table.build_sql(
            schema=os.environ.get("DATABASE_SCHEMA"), load_method=load_method
        )
        binds = table.generate_binds(max_value_in_target=date_from)

        df = db2_conn.get_rows_as_dataframe(query=query, binds=binds)

        if table_exists_in_bq:
            write_disposition = "WRITE_APPEND"

        else:
            write_disposition = "WRITE_TRUNCATE"

    else:
        raise ValueError(
            f"Ukjent table_type {table.table_type} for tabell {table.name}."
        )

    if len(df) > 0:
        bq_client.put_dataframe(df, table_id, write_disposition, table.table_type)
        # table_type blir brukt til å sette time partitions på fak tabeller i job config.

    elif load_method == "delta":
        print(f"Ingen nye rader å laste for tabell {table.name}")
    else:
        print(f"Ingen rader hentet fra kildetabell {table.name}")


def main():
    # bq_client = create_bq_client(local_dev=local)
    os.environ["GOOGLE_CLOUD_PROJECT"] = "utsikt-dev-3609"
    bq_client = BQConnector()
    # db2_conn = create_db2_conn(local_dev=local)
    db2_conn = DB2Connector(
        database_name=os.environ["DATABASE_NAME"],
        username=os.environ["DATABASE_USERNAME"],
        password=os.environ["DATABASE_PASSWORD"],
        port=os.environ["DATABASE_PORT"],
        host=os.environ["DATABASE_HOST"],
    )

    for table in tables:
        db2_to_bq(table, bq_client, db2_conn)


def db2_to_bq_pseudo():
    # sjekker tabell type
    # Hvis tabell type = "dim" -> full last
    # Hvis tabell type = "fak" -> Må sette fra_dato
    #   Hvis tabell finnes i BQ hentes fra_dato i BQ
    #   Hvis ikke sette fra_data = i dag - minus 2 år
    pass


if __name__ == "__main__":
    main()
    # TODO:
    # - Mer av logikken til klassen Tabel
    # - Gjør om config tables til BQ Schema fields
    # - Skrive tabellbeskrivelse til BQ
