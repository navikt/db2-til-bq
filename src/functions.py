from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from typing import Union, Any
from yaml import safe_load

from src.bigquery_connector import BQConnector
from src.db2_connector import DB2Connector
from src.class_table import DimTable, FakTable, TableType
from src.logger import Logger
from src.env_handler import EnvHandler
from src.config_loader import TableModel


def set_and_check_envs(source_name: str) -> None:
    env_handler = EnvHandler(source_name=source_name)
    env_handler.load_envs()
    env_handler.check_envs()


def load_config_tables(
    config_path: str,
) -> list[Union[DimTable, FakTable]]:
    with open(config_path) as file:
        tables = safe_load(file)

    table_objs = []
    for table in tables["tables"]:
        table_objs.append(TableModel.from_dict(table).to_table_object())

    return table_objs


def get_from_datetime(
    bq_client: BQConnector, table: Union[DimTable, FakTable], table_exists_in_bq: bool
) -> str:
    if table_exists_in_bq:
        max_query = f"SELECT MAX({table.check_col}) FROM {table.bq_table_id}"
        from_date_dict: dict[str, Any] = bq_client.get_rows(query=max_query)[0]
        from_date = list(from_date_dict.values())[0]

    else:
        from_date = datetime.today() - timedelta(days=730)

    return from_date.strftime("%Y-%m-%d %H:%M:%S.%f")


def generate_limits(start_datetime: datetime) -> list[date]:
    start_date = start_datetime.date().replace(day=1)
    end_date = datetime.today().date() + relativedelta(months=1)

    date_list = []

    current = start_date
    while current <= end_date:
        date_list.append(current)
        current += relativedelta(months=1)

    return date_list


def delete_table(
    table: Union[DimTable, FakTable], bq_client: BQConnector, logger: Logger
) -> None:
    table_name = table.bq_table_id
    table_dataset = table.bq_dataset
    bq_client.delete_table(table_name=table_name, dataset=table_dataset, logger=logger)


def create_datasets(
    datasets: list[str], bq_connector: BQConnector, logger: Logger
) -> None:
    for dataset in datasets:
        bq_connector.create_dataset(dataset, logger=logger)


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


def update_desc(logger: Logger, source_name: str):
    """
    Kjøres for å oppdatere tabell og kolonnekommentarer. #TODO få en jobb som kjører denne jevnlig
    """
    set_and_check_envs(source_name=source_name)

    tables = load_config_tables(config_path=f"{source_name}_tables.yaml")

    logger.info("Oppdater beskrivelse og schema i alle BigQuery-tabellene")

    bq_client = BQConnector()
    for table in tables:
        bq_client.update_table_and_col_descriptions(
            table_id=table.bq_table_id,
            desc=table.description,
            schema=table.cols,
            logger=logger,
        )
