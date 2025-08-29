from dataclasses import dataclass

@dataclass
class Table:
    """klasse for å lagre informasjon om kildetabellene våre"""
    name: str # tabellnavn
    columns: list # kolonner vi skal hente
    load_method: str # 'full' eller 'delta'
    col_descriptions: list = None # utsikts beskrivelser av kolonner, og begrunnelse for hvorfor vi henter dem
    check_col: str = None # kolonne vi sjekker for endringer ved deltalast, og en kolonne som sjekker om det trengs full last

    def set_load_method_to_full(self):
        self.load_method = 'full'

    def build_sql(self, schema: str, maxval_tgt = None) -> str:
        query = f"""SELECT {', '.join(self.columns)} 
                FROM {schema}.{self.name}
        """
        if self.load_method == 'delta':
            query = query + f"WHERE {self.check_col} > {maxval_tgt}"
        return query

