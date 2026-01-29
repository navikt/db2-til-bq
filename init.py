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

        logger.info(f"Running for month: {dates[i].strftime("%B")}-{dates[i].year}")

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
        db2_conn = DB2Connector.create_connector_from_envs()
        db2_to_bq(table=table, bq_client=bq_client, db2_conn=db2_conn, logger=logger)
        db2_conn.close()


if __name__ == "__main__":
    logs = Logger(name="db2-til-bq-init")
    main(logger=logs, table_name="t_vent_detalj")
