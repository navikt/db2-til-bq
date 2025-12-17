from datetime import datetime, timedelta
from typing import Union

from src.bigquery_connector import BQConnector
from src.class_table import DimTable, FakTable
from src.logger import Logger
from src.env_handler import EnvHandler


def set_and_check_envs(gcp_project_id: str) -> None:
    env_handler: EnvHandler = EnvHandler(gcp_project_id=gcp_project_id)
    env_handler.load_envs()
    env_handler.check_envs()


def get_from_datetime(bq_client: BQConnector, table: Union[DimTable, FakTable], table_exists_in_bq: bool) -> datetime:

    if table_exists_in_bq:
        max_query = f"SELECT MAX({table.check_col}) FROM {table.bq_table_id}"
        from_date = bq_client.get_rows_as_dataframe(max_query).iloc[0, 0]

    else:
        from_date = datetime.today() - timedelta(days=730)


    return from_date

def delete_table(table: Union[DimTable, FakTable], bq_client: BQConnector, logger: Logger) -> None:
    table_name = table.bq_table_id
    table_dataset = table.bq_dataset
    bq_client.delete_table(table_name=table_name, dataset=table_dataset, logger=logger)


def create_datasets(datasets: list[str], bq_connector: BQConnector, logger:Logger) -> None:
    for dataset in datasets:
        bq_connector.create_dataset(dataset, logger=logger)


if __name__ == "__main__":
    print("This is a module, not to be run directly")
