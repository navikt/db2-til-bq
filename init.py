#!/usr/bin/env python3
from typing import Union
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

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

    base_query = table.build_sql_db2()
    binds = table.generate_binds()
    job_config = table.make_bq_load_job_config()

    chunk_size = 1000000
    total_rows = 0

    for chunk in db2_conn.get_chunks(
        chunk_size=chunk_size, base_query=base_query, binds=binds
    ):
        if len(chunk) > 0:
            bq_client.put_rows_alt(
                chunk, table_id=table.bq_table_id, job_config=job_config
            )

        total_rows += len(chunk)
        logger.info(
            f"Total rows: {total_rows} and chunk of size: {len(chunk)} rows was written to {table.name.upper()}"
        )


def main(logger: Logger):
    set_and_check_envs()

    tables = load_config_tables()
    bq_client = BQConnector()

    for table in tables:
        db2_conn = DB2Connector.create_connector_from_envs()
        db2_to_bq(table=table, bq_client=bq_client, db2_conn=db2_conn, logger=logger)
        db2_conn.close()



def generate_limits(start_datetime: datetime) -> list[date]:

    start_date = start_datetime.date().replace(day=1)
    end_date = datetime.today().date() + relativedelta(months=1)

    date_list = []

    current = start_date
    while current <= end_date:
        date_list.append(current)
        current += relativedelta(months=1)

    return date_list


if __name__ == "__main__":
    dates = generate_limits(start_datetime=datetime(year=2024, month=4, day=14))
    print(dates)
