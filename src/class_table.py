from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class Table:
    """klasse for å lagre informasjon om kildetabellene våre"""

    name: str  # tabellnavn
    table_type: str  # type tabell, dim eller fak. Brukes for å styre lastemetode.
    cols: list
    # columns: list  # kolonner vi skal hente
    # col_descriptions: list = (
    #    None  # utsikts beskrivelser av kolonner, og begrunnelse for hvorfor vi henter dem
    # )
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

    def build_sql_init(self, schema: str, offset: int, chunk_size: int) -> str:
        query = f"""SELECT {', '.join(self.columns)} 
                FROM {schema}.{self.name}
                OFFSET {offset} ROWS 
                FETCH NEXT {chunk_size} ROWS ONLY
                """
        return query
