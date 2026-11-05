import os
from typing import Any
from enum import Enum
from abc import ABC, abstractmethod

from google.cloud.bigquery import SchemaField, LoadJobConfig, table


class TableType(Enum):
    """Angir om en tabell er en dimensjons- eller faktatabell.

    Attributes
    ----------
    DIM : TableType
        Dimensjonstabell.
    FAK : TableType
        Faktatabell.
    """

    DIM = "dim"
    FAK = "fak"


class BaseTable(ABC):
    """Samler egenskaper og felles operasjoner for DB2- og BigQuery-tabeller.

    Attributes
    ----------
    name : str
        Tabellnavnet i DB2.
    description : str
        Tabellbeskrivelsen.
    cols : list[SchemaField]
        BigQuery-feltene tabellen består av.
    db2_schema : str
        DB2-skjemaet fra miljøvariabelen ``DATABASE_SCHEMA``.
    bq_dataset : str
        De to første tegnene i DB2-skjemaet.
    bq_table_id : str
        BigQuery-tabell-ID på formen datasett.tabell.
    table_type : TableType
        Tabelltypen.
    """

    def __init__(
        self,
        name: str,
        description: str,
        cols: list[SchemaField],
        table_type: TableType,
    ) -> None:
        """Opprett en tabell og les tilknyttede skjema- og datasettverdier fra miljøet.

        Parameters
        ----------
        name : str
            Tabellnavnet i DB2.
        description : str
            Tabellbeskrivelsen.
        cols : list[SchemaField]
            BigQuery-feltene tabellen består av.
        table_type : TableType
            Tabelltypen.

        Raises
        ------
        KeyError
            Hvis miljøvariabelen ``DATABASE_SCHEMA`` ikke er satt.
        """
        self._name = name
        self._description = description
        self._cols = cols
        self._table_type = table_type
        self._db2_schema = None
        self._bq_dataset = None
        self._bq_table_id = None

        self._set_envs()

    def _set_envs(self) -> None:
        """Les DB2-skjemaet og utled BigQuery-datasettet og tabell-ID-en.

        Raises
        ------
        KeyError
            Hvis miljøvariabelen ``DATABASE_SCHEMA`` ikke er satt.
        """
        self._db2_schema: str = os.environ["DATABASE_SCHEMA"]
        self._bq_dataset: str = self._db2_schema[:2]
        self._bq_table_id = f"{self._bq_dataset}.{self._name}"

    @property
    def name(self) -> str:
        """Returner tabellnavnet i DB2.

        Returns
        -------
        str
            Tabellnavnet.
        """
        return self._name

    @property
    def description(self) -> str:
        """Returner tabellbeskrivelsen.

        Returns
        -------
        str
            Beskrivelsen.
        """
        return self._description

    @property
    def cols(self) -> list[SchemaField]:
        """Returner BigQuery-feltene for tabellen.

        Returns
        -------
        list[SchemaField]
            Tabellens felt.
        """
        return self._cols

    @property
    def db2_schema(self) -> str:
        """Returner DB2-skjemaet fra miljøvariabelen.

        Returns
        -------
        str
            DB2-skjemaet.
        """
        return self._db2_schema

    @property
    def bq_dataset(self) -> str:
        """Returner de to første tegnene i DB2-skjemaet som BigQuery-datasett.

        Returns
        -------
        str
            BigQuery-datasettet.
        """
        return self._bq_dataset

    @property
    def bq_table_id(self) -> str:
        """Returner BigQuery-tabell-ID-en på formen datasett.tabell.

        Returns
        -------
        str
            BigQuery-tabell-ID-en.
        """
        return self._bq_table_id

    @property
    def table_type(self) -> TableType:
        """Returner tabelltypen.

        Returns
        -------
        TableType
            Tabelltypen.
        """
        return self._table_type

    @abstractmethod
    def build_sql_db2(self) -> str:
        """Bygg en SQL-spørring for å hente tabellens data fra DB2.

        Returns
        -------
        str
            SQL-spørringen.
        """
        raise NotImplementedError()

    def _build_sql_db2(self) -> str:
        """Bygg en enkel SELECT-spørring med alle tabellens kolonner.

        Returns
        -------
        str
            SELECT-spørring uten filtrering eller sortering.
        """
        column_names: str = ",".join([col.name for col in self.cols])
        query = f"""SELECT {column_names} FROM {self.db2_schema}.{self.name} """
        return query

    @abstractmethod
    def make_bq_load_job_config(self) -> LoadJobConfig:
        """Opprett konfigurasjon for lasting av data til BigQuery.

        Returns
        -------
        LoadJobConfig
            Konfigurasjon for BigQuery-lastejobben.
        """
        raise NotImplementedError()

    def _make_bq_load_job_config(self, write_disposition: str) -> LoadJobConfig:
        """Opprett en BigQuery-lastekonfigurasjon med tabellens skjema.

        Parameters
        ----------
        write_disposition : str
            BigQuery-strategien for skriving til måltabellen.

        Returns
        -------
        LoadJobConfig
            Konfigurasjon med tabellens skjema og ``CREATE_IF_NEEDED``.
        """
        schema = self.cols
        create_disposition = "CREATE_IF_NEEDED"

        job_config = LoadJobConfig(
            schema=schema,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
        )

        return job_config

    @abstractmethod
    def generate_binds(self) -> dict[int, Any]:
        """Lag parameterbindinger for DB2-spørringen.

        Returns
        -------
        dict[int, Any]
            Bindingsverdier indeksert etter parameterposisjon.
        """
        raise NotImplementedError()

    @staticmethod
    def _generate_binds() -> dict[int, Any]:
        """Returner en tom samling parameterbindinger.

        Returns
        -------
        dict[int, Any]
            Tomt oppslagsverk for bindingsverdier.
        """
        return {}


class DimTable(BaseTable):
    """Representerer en dimensjonstabell som erstattes ved hver lasting."""

    def __init__(self, name: str, description: str, cols: list[SchemaField]) -> None:
        """Opprett en dimensjonstabell.

        Parameters
        ----------
        name : str
            Tabellnavnet i DB2.
        description : str
            Tabellbeskrivelsen.
        cols : list[SchemaField]
            BigQuery-feltene tabellen består av.

        Raises
        ------
        KeyError
            Hvis miljøvariabelen ``DATABASE_SCHEMA`` ikke er satt.
        """
        super().__init__(
            name=name, description=description, cols=cols, table_type=TableType.DIM
        )

    def build_sql_db2(self) -> str:
        """Bygg en SELECT-spørring for alle kolonnene i dimensjonstabellen.

        Returns
        -------
        str
            SQL-spørringen.
        """
        return self._build_sql_db2()

    def make_bq_load_job_config(self) -> LoadJobConfig:
        """Opprett lastekonfigurasjon som overskriver BigQuery-tabellen.

        Returns
        -------
        LoadJobConfig
            Konfigurasjon med ``WRITE_TRUNCATE`` og ``CREATE_IF_NEEDED``.
        """
        return self._make_bq_load_job_config(write_disposition="WRITE_TRUNCATE")

    def generate_binds(self) -> dict[int, Any]:
        """Returner ingen parameterbindinger for dimensjonstabellen.

        Returns
        -------
        dict[int, Any]
            Tomt oppslagsverk.
        """
        return self._generate_binds()


class FakTable(BaseTable):
    """Representerer en faktatabell som lastes inkrementelt og dagspartisjoneres.

    Attributes
    ----------
    check_col : str
        Kolonnen som brukes til å avgrense nye rader og partisjonere i BigQuery.
    order_cols : list[str]
        Kolonner som brukes i SQL-sorteringen.
    from_datetime : str
        Nedre tidsgrense for uttrekket, eller ``None`` hvis den ikke er satt.
    """

    def __init__(
        self,
        name: str,
        description: str,
        cols: list[SchemaField],
        check_col: str,
        order_cols: list[str],
    ) -> None:
        """Opprett en faktatabell med kontrollkolonne og sorteringskolonner.

        Parameters
        ----------
        name : str
            Tabellnavnet i DB2.
        description : str
            Tabellbeskrivelsen.
        cols : list[SchemaField]
            BigQuery-feltene tabellen består av.
        check_col : str
            Kolonnen som brukes til å avgrense nye rader.
        order_cols : list[str]
            Kolonner som brukes i SQL-sorteringen.

        Raises
        ------
        KeyError
            Hvis miljøvariabelen ``DATABASE_SCHEMA`` ikke er satt.
        """
        super().__init__(
            name=name, description=description, cols=cols, table_type=TableType.FAK
        )
        self._check_col = check_col
        self._order_cols = order_cols
        self._from_datetime = None

    @property
    def check_col(self) -> str:
        """Returner kolonnen som brukes til å avgrense nye rader.

        Returns
        -------
        str
            Kontrollkolonnen.
        """
        return self._check_col

    @property
    def order_cols(self) -> list[str]:
        """Returner kolonnene som brukes i SQL-sorteringen.

        Returns
        -------
        list[str]
            Sorteringskolonnene.
        """
        return self._order_cols

    @property
    def from_datetime(self) -> str:
        """Returner nedre tidsgrense for faktatabellens uttrekk.

        Returns
        -------
        str
            Tidsgrensen, eller ``None`` hvis den ikke er satt.
        """
        return self._from_datetime

    @from_datetime.setter
    def from_datetime(self, from_datetime: str) -> None:
        """Sett nedre tidsgrense for faktatabellens uttrekk.

        Parameters
        ----------
        from_datetime : str
            Verdien som bindes til spørringens parameter.
        """
        self._from_datetime = from_datetime

    def build_sql_db2(self) -> str:
        """Bygg en spørring som filtrerer på kontrollkolonnen og sorterer rader.

        Returns
        -------
        str
            SELECT-spørring med et posisjonelt parameter og ORDER BY.
        """
        base_query = self._build_sql_db2()
        col_query = f"WHERE {self._check_col} > ?"
        order_by = f"ORDER BY {','.join([col for col in self.order_cols])}"

        return base_query + col_query + order_by

    def make_bq_load_job_config(self) -> LoadJobConfig:
        """Opprett en tilleggslastekonfigurasjon med dagspartisjonering.

        Returns
        -------
        LoadJobConfig
            Konfigurasjon med ``WRITE_APPEND``, tabellskjema og dagspartisjonering
            på kontrollkolonnen.
        """
        base_job_config = self._make_bq_load_job_config(
            write_disposition="WRITE_APPEND"
        )
        _field: str = self.check_col

        partition = table.TimePartitioning(
            type_="DAY", field=_field, expiration_ms=1000 * 60 * 60 * 24 * 730
        )

        base_job_config.time_partitioning = partition
        return base_job_config

    def generate_binds(self) -> dict[int, Any]:
        """Lag parameterbindingen for nedre tidsgrense.

        Returns
        -------
        dict[int, Any]
            Binding med tidsgrensen på parameterposisjon 1.
        """
        return {1: self.from_datetime}
