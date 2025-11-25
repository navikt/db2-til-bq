import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List
from google.cloud.bigquery import SchemaField
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

    @staticmethod
    def generate_binds(max_value_in_target=None) -> Dict[int, Any]:
        binds = {1: max_value_in_target}

        return binds
