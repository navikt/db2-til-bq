from datetime import datetime, timedelta

from src.bigquery_connector import BQConnector
from src.class_table import Table


def get_from_datetime(bq_client: BQConnector, table: Table, table_exists_in_bq: bool) -> datetime:

    if table_exists_in_bq:
        max_query = f"SELECT MAX({table.check_col}) FROM {table.bq_table_id}"
        from_date = bq_client.get_rows_as_dataframe(max_query).iloc[0, 0]

    else:
        from_date = datetime.today() - timedelta(days=730)


    return from_date


if __name__ == "__main__":
    print("This is a module, not to be run directly")
