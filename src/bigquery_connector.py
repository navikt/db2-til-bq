from typing import List, Dict, Any
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pandas import DataFrame


class BQConnector:
    def __init__(self):
        self.client: bigquery.Client = self._create_client()

    def _execute_query(self, query: str) -> bigquery.QueryJob:
        """
        Privat metode som brukes for å kjøre spørring mot BQ.

        Args:
            query (str): SQL spørring som skal kjøre

        Returns (bigquery.QueryJob): En QueryJob som sendes videre for behandling.

        """
        return self.client.query(query=query)

    def get_rows(self, query: str) -> List[Dict[str, Any]]:
        """
        Metode for å kjøre spørring med resultater og som skal formaters som en liste av dicts.
        Args:
            query (str): SQL spørring som skal kjøres

        Returns List[Dict[str, Any]: En liste av dicts med keys som kolonnenavn og values som verdi til kolonne.

        """
        query_job = self._execute_query(query=query)
        return self._format_results(query_job=query_job)

    def put_dataframe(
        self, df: DataFrame, table_id: str, job_config: bigquery.LoadJobConfig
    ) -> None:
        """
        Laster opp en Dataframe til gitt tabell og write_disposition

        Args:
            df (DataFrame): Dataframen som skal lastes opp
            table_id (str): Tabellen i BQ som skal skrives til
            write_disposition (str): Enten "WRITE_APPEND" eller "WRITE_TRUNCATE"
            table_type (str): Enten "fak" eller "dim"

        Returns (None): Skriver ut resultater av jobben.

        """
        job = self.client.load_table_from_dataframe(
            dataframe=df, destination=table_id, job_config=job_config
        )

        job.result()

    def get_rows_as_dataframe(self, query: str) -> DataFrame:
        """
        Metode for å kjøre spørring med resultater som returneres som en pandas DataFrame
        Args:
            query (str): Spørring som skal kjøres

        Returns (DataFrame): DataFrame med resultater

        """
        rows = self.get_rows(query=query)
        return DataFrame(data=rows)

    @staticmethod
    def _create_client() -> bigquery.Client:
        """
        Lager en bigquery klient. En av to envs må være satt

            - GOOGLE_CLOUD_PROJECT: prosjekt id som skal brukes for lokal utvikling

            - GOOGLE_APPLICATION_CREDENTIALS: Sti til google credentials fil. Kan brukes til  utvikling
                                              lokalt eller på NAIS.

        Returns (bigquery.Client): Oppretter bigquery klient

        """
        return bigquery.Client()

    @staticmethod
    def _create_write_job_config(
        write_disposition: str, table_type: str
    ) -> bigquery.LoadJobConfig:
        """
        Lager en bigquery job config med gitt write disposition.
        Args:
            write_disposition (str): enten "WRITE_APPEND" eller "WRITE_TRUNCATE"

        Returns (bigquery.LoadJobConfig): Config for å kjøre jobben

        """
        if table_type == "fak":
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition=write_disposition,
                create_disposition="CREATE_IF_NEEDED",
                time_partitioning=bigquery.table.TimePartitioning(  # sett opp måte å kun partisjonere tabellene som har data som det skal slettes for!!!!!!
                    type_="DAY",
                    field="tidspkt_reg",  # TODO parametere for field
                    expiration_ms=1000
                    * 60
                    * 60
                    * 24
                    * 730,  # Data som er 730 dager = 2 år gammel slettes automatisk (som definert i behandlingen)
                ),
            )
        else:  # dim
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition=write_disposition,
                create_disposition="CREATE_IF_NEEDED",
            )

        return job_config

    @staticmethod
    def _format_results(query_job: bigquery.QueryJob) -> List[Dict[str, Any]]:
        """
        Statisk metode for å formatere resultater fra en QueryJob.
        Args:
            query_job (bigquery.QueryJob): QueryJob med resultater fra en spørring

        Returns:

        """
        results = query_job.result()
        return [{key: value for key, value in row.items()} for row in results]

    def check_if_table_exists_in_bq(self, table_id: str) -> bool:
        # Check if table exists in BQ, and set appropriate load method.
        try:
            self.client.get_table(table_id)  # Make an API request.
            table_exists_in_bq = True
            print("Tabellen {} finnes i BQ.".format(table_id))
        except NotFound:
            table_exists_in_bq = False
            print("Tabellen {} finnes ikke i BQ.".format(table_id))

        return table_exists_in_bq
