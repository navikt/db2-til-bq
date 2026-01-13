from datetime import datetime, timedelta
from typing import Union

from yaml import safe_load

from src.bigquery_connector import BQConnector
from src.class_table import DimTable, FakTable
from src.logger import Logger
from src.env_handler import EnvHandler
from src.config_loader import TableModel


def set_and_check_envs() -> None:
    env_handler = EnvHandler()
    env_handler.load_envs()
    env_handler.check_envs()


def load_config_tables(
    config_path: str = "config_tables.yaml",
) -> list[Union[DimTable, FakTable]]:
    with open(config_path) as file:
        tables = safe_load(file)

    table_objs = []
    for table in tables["tables"]:
        table_objs.append(TableModel.from_dict(table).to_table_object())

    return table_objs


def get_from_datetime(
    bq_client: BQConnector, table: Union[DimTable, FakTable], table_exists_in_bq: bool
) -> datetime:
    if table_exists_in_bq:
        max_query = f"SELECT MAX({table.check_col}) FROM {table.bq_table_id}"
        from_date = bq_client.get_rows_as_dataframe(max_query).iloc[0, 0]

    else:
        from_date = datetime.today() - timedelta(days=365)

    return from_date


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


if __name__ == "__main__":
    print("This is a module, not to be run directly")
