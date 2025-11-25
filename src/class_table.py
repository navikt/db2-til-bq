import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List
from google.cloud.bigquery import SchemaField, LoadJobConfig, table
from enum import Enum


class TableType(Enum):
    DIM = "dim"
    FAK = "fak"


@dataclass
class Table:
    """klasse for å lagre informasjon om kildetabellene våre"""

    name: str  # tabellnavn
    table_type: TableType  # type tabell, dim eller fak. Brukes for å styre lastemetode.
    cols: List[SchemaField]
    check_col: str = None  # kolonne vi sjekker for endringer ved deltalast
    db2_schema: str = field(init=False)
    bq_dataset: str = field(init=False)
    bq_table_id: str = field(init=False)

    def __post_init__(self):
        self.db2_schema = os.environ.get("DATABASE_SCHEMA")
        self.bq_dataset = self.db2_schema[:2] + "_" + self.db2_schema[-2:]
        self.bq_table_id = f"{self.bq_dataset}.{self.name}"

    def build_sql_db2(self) -> str:
        """funksjon som bygger spørring mot DB2"""
        column_names = [col.name for col in self.cols]
        query = f"""SELECT {', '.join(column_names)} 
                FROM {self.db2_schema}.{self.name}
        """
        if self.table_type == TableType.FAK:
            query = query + f"WHERE {self.check_col} > ? "

        return query

    def make_bq_load_job_config(self, write_disposition: str) -> LoadJobConfig:

        schema = self.cols
        create_disposition = "CREATE_IF_NEEDED"

        job_config = LoadJobConfig(schema=schema,
                                   write_disposition=write_disposition,
                                   create_disposition=create_disposition)

        if self.table_type == TableType.FAK:
            job_config.time_partitioning = table.TimePartitioning(
                type_="DAY",
                field="tidspkt_reg",
                expiration_ms=1000* 60* 60 * 24* 730)


        return job_config

    @staticmethod
    def generate_binds(from_datetime = None) -> Dict[int, Any]:
        if from_datetime:
            binds = {1: from_datetime}
        else:
            binds = {}

        return binds
