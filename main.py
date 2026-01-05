#!/usr/bin/env python3
from typing import Union

from src.bigquery_connector import BQConnector
from src.db2_connector import DB2Connector
from src.functions import get_from_datetime, set_and_check_envs, load_config_tables
from src.class_table import DimTable, FakTable, TableType
from src.logger import Logger


def db2_to_bq(
    table: Union[DimTable, FakTable],
    bq_client: BQConnector,
    db2_conn: DB2Connector,
    logger: Logger,
):
    logger.info(
        f"Processing table: {table.name.upper()} of type:{table.table_type.value.upper()}"
    )

    if table.table_type == TableType.FAK:
        table_exists_in_bq = bq_client.check_if_table_exists_in_bq(
            table_id=table.bq_table_id
        )
        logger.info(f"{table.name.upper()} exists: {table_exists_in_bq}")
        table.from_datetime = get_from_datetime(
            bq_client=bq_client, table=table, table_exists_in_bq=table_exists_in_bq
        )

    query = table.build_sql_db2()
    binds = table.generate_binds()

    total_number_rows = 0
    for df in db2_conn.get_chunks(query=query, binds=binds, chunk_size=100000):
        if len(df) > 0:
            df.columns = df.columns.str.lower()
            job_config = table.make_bq_load_job_config()
            bq_client.put_dataframe(df, table_id=table.bq_table_id, job_config=job_config)
            total_number_rows += len(df)

        logger.info(f"Chunk of size {len(df)} was written to {table.name.upper()}")

    logger.info(f"Total number of rows written to {table.name.upper()} was {total_number_rows}")


def main(logger: Logger):
    set_and_check_envs()

    tables = load_config_tables()

    bq_client = BQConnector()
    db2_conn = DB2Connector.create_connector_from_envs()

    for table in tables:
        db2_to_bq(table=table, bq_client=bq_client, db2_conn=db2_conn, logger=logger)


def update_desc(logger: Logger):
    set_and_check_envs()

    tables = load_config_tables()

    logger.info("Oppdater beskrivelse og schema i alle BigQuery-tabellene")

    bq_client = BQConnector()
    for table in tables:
        bq_client.update_table_and_col_descriptions(
            table_id=table.bq_table_id,
            desc=table.description,
            schema=table.cols,
            logger=logger,
        )


if __name__ == "__main__":
    logs = Logger(name="db2-til-bq")
    main(logger=logs)
    # update_desc(logger=logs)  # Kjøres for å oppdatere tabell og kolonnekommentarer
