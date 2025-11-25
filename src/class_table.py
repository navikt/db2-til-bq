from dataclasses import dataclass
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

    def build_sql(self, schema: str, load_method) -> str:
        column_names = [col.name for col in self.cols]
        query = f"""SELECT {', '.join(column_names)} 
                FROM {schema}.{self.name}
        """
        if load_method == "delta":
            query = query + f"WHERE {self.check_col} > ? "

        return query

    @staticmethod
    def generate_binds(max_value_in_target=None) -> Dict[int, Any]:
        binds = {1: max_value_in_target}

        return binds
