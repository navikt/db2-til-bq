#!/usr/bin/env python3
from typing import Union
from datetime import datetime


from src.bigquery_connector import BQConnector
from src.db2_connector import DB2Connector
from src.functions import (
    get_from_datetime,
    set_and_check_envs,
    load_config_tables,
    generate_limits,
)
from src.class_table import DimTable, FakTable, TableType
from src.logger import Logger


def db2_to_bq(
    table: Union[DimTable, FakTable],
    bq_client: BQConnector,
    db2_conn: DB2Connector,
    logger: Logger,
):
    """Les tabellen fra DB2 i datointervaller og skriv radene til BigQuery.

    Kontrollerer først om måltabellen finnes, beregner startdato og sletter
    eksisterende rader fra det første genererte datointervallet når tabellen
    finnes. Henter deretter rader i månedsintervaller og laster dem i bolker.

    Parameters
    ----------
    table : DimTable or FakTable
        Tabellbeskrivelse som angir kilde, mål, datokolonne og sortering.
    bq_client : BQConnector
        Klient for kontroll, sletting og lasting i BigQuery.
    db2_conn : DB2Connector
        DB2-tilkobling som henter radbolker.
    logger : Logger
        Logger for statusmeldinger.

    Returns
    -------
    None
        Funksjonen returnerer ikke en verdi.

    Raises
    ------
    ValueError
        Hvis ``table.from_datetime`` ikke kan tolkes med forventet tidsformat.
    """
    logger.info(
        f"Processing table: {table.name.upper()} of type:{table.table_type.value.upper()}"
    )

    table_exists_in_bq = bq_client.check_if_table_exists_in_bq(
        table_id=table.bq_table_id
    )
    logger.info(f"{table.name.upper()} exists: {table_exists_in_bq}")
    table.from_datetime = get_from_datetime(
        bq_client=bq_client, table=table, table_exists_in_bq=table_exists_in_bq
    )
    start_datetime = datetime.strptime(table.from_datetime, "%Y-%m-%d %H:%M:%S.%f")

    dates = generate_limits(start_datetime=start_datetime)

    if table_exists_in_bq:
        bq_client.query(
            f"delete from {table.bq_table_id} where {table.check_col} >= DATE('{dates[0]}')"
        )

        logger.info(f"Deleted rows after {dates[0]}")

    column_names: str = ",".join([col.name for col in table.cols])
    base_query = f"SELECT {column_names} FROM {table.db2_schema}.{table.name} "
    order_query = f"ORDER BY {','.join(table.order_cols)}"

    job_config = table.make_bq_load_job_config()

    for i in range(len(dates) - 1):
        first_date = dates[i].strftime("%Y-%m-%d")
        last_date = dates[i + 1].strftime("%Y-%m-%d")
        date_query = f"WHERE {table.check_col} >= ? AND {table.check_col} < ? "
        query = base_query + date_query + order_query

        binds = {1: first_date, 2: last_date}

        logger.info(f"Running for month: {dates[i].strftime('%B')}-{dates[i].year}")

        chunk_size = 1_000_000
        total_rows = 0

        for chunk in db2_conn.get_chunks(
            chunk_size=chunk_size, base_query=query, binds=binds
        ):
            if len(chunk) > 0:
                bq_client.put_rows_alt(
                    chunk, table_id=table.bq_table_id, job_config=job_config
                )

            total_rows += len(chunk)
            logger.info(
                f"Total rows: {total_rows} and chunk of size: {len(chunk)} rows was written to {table.name.upper()}"
            )


def main(logger: Logger, table_name: str = None):
    """Velg faktatabeller og kjør DB2-til-BigQuery-jobben for dem.

    Parameters
    ----------
    logger : Logger
        Logger som brukes for statusmeldinger.
    table_name : str, optional
        Navn som sammenlignes med små bokstaver mot tabellnavnene. Verdien
        konverteres ikke selv til små bokstaver.

    Returns
    -------
    None
        Funksjonen returnerer ikke en verdi.

    Notes
    -----
    Slik funksjonene er definert, krever ``set_and_check_envs`` argumentet
    ``source_name`` og ``load_config_tables`` argumentet ``config_path``.
    Kallene her utelater begge argumentene. Derfor stopper standardkjøringen
    med ``TypeError`` ved det første kallet, før tabeller behandles.
    """
    set_and_check_envs()

    tables = load_config_tables()
    bq_client = BQConnector()

    tables_to_run = [
        fak_table for fak_table in tables if fak_table.table_type == TableType.FAK
    ]

    if table_name:
        tables_to_run = [
            table for table in tables_to_run if table.name.lower() == table_name
        ]

    for table in tables_to_run:
        logger.info(f"Starting to process table: {table.name}")
        db2_conn = DB2Connector.create_connector_from_envs()
        db2_to_bq(table=table, bq_client=bq_client, db2_conn=db2_conn, logger=logger)
        db2_conn.close()


if __name__ == "__main__":
    logs = Logger(name="db2-til-bq-init-OS")
    main(logger=logs, table_name="t_vent_stoppnivaa")
