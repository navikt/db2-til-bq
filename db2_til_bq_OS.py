#!/usr/bin/env python3
from src.bigquery_connector import BQConnector
from src.db2_connector import DB2Connector
from src.functions import set_and_check_envs, load_config_tables, db2_to_bq
from src.logger import Logger


def main(logger: Logger):
    set_and_check_envs(source_name="OS")

    tables = load_config_tables(config_path="tables_OS.yaml")
    bq_client = BQConnector()

    for table in tables:
        db2_conn = DB2Connector.create_connector_from_envs()
        db2_to_bq(table=table, bq_client=bq_client, db2_conn=db2_conn, logger=logger)
        db2_conn.close()


if __name__ == "__main__":
    logs = Logger(name="db2_til_bq_OS")
    main(logger=logs)
