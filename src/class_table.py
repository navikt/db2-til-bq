from dataclasses import dataclass

@dataclass
class Table:
    """klasse for å lagre informasjon om kildetabellene våre"""
    name: str
    columns: list
    col_descriptions: list = None
    check_col: str = None

    def build_sql(self, schema: str, maxval_tgt = None) -> str:
        query = f"""SELECT {', '.join(self.columns)} 
                FROM {schema}.{self.name}
        """
        if self.check_col:
            query = query + f"WHERE {self.check_col} > {maxval_tgt}"
        return query

