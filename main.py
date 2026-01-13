#!/usr/bin/env python3
from typing import Union, Iterator
from pandas import DataFrame
from src.bigquery_connector import BQConnector
from src.db2_connector import DB2Connector
from src.functions import get_from_datetime, set_and_check_envs, load_config_tables
from src.class_table import DimTable, FakTable, TableType
from src.logger import Logger


def get_chunks(
    conn: DB2Connector, chunk_size: int, base_query: str, binds: dict
) -> Iterator[DataFrame]:
    offset = 0
    # base_query = "SELECT beregnings_id,dato_beregnet,kode_faggruppe,tidspkt_reg FROM OS314T1.t_vent_beregning WHERE tidspkt_reg > Timestamp('2026-01-07 20:07:37.276988')"
    done = False
    while not done:

        query = (
            f"""{base_query} OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"""
        )
        print(query)
        print(binds)

        df = conn.get_rows_as_dataframe(query=query)  # , binds=binds
        offset += chunk_size

        if len(df) < chunk_size:
            done = True

        yield df


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

    if (table.table_type == TableType.DIM) or table_exists_in_bq:
        df = db2_conn.get_rows_as_dataframe(query=query, binds=binds)

        if len(df) > 0:
            df.columns = df.columns.str.lower()
            job_config = table.make_bq_load_job_config()
            bq_client.put_dataframe(
                df, table_id=table.bq_table_id, job_config=job_config
            )

        logger.info(f"{len(df)} rows was written to {table.name.upper()}")
    else:  # tabellen eksisterer ikke og er FAK
        # dette må endres til none hvis vi skal bruke indekser istedet
        db2_conn.exec_immediate("SET CURRENT QUERY ACCELERATION ALL")
        job_config = table.make_bq_load_job_config()

        chunk_size = 1000000

        total_rows = 0
        for chunk in get_chunks(
            conn=db2_conn, chunk_size=chunk_size, base_query=query, binds=binds
        ):
            chunk.columns = chunk.columns.str.lower()
            bq_client.put_dataframe(
                chunk, table_id=table.bq_table_id, job_config=job_config
            )
            total_rows += len(chunk)
            logger.info(
                f"Total rows: {total_rows} and chunk of size:{len(chunk)} rows was written to {table.name.upper()}"
            )


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
