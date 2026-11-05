from typing import List, Dict, Any
from datetime import datetime, date

import json
import io

from google.cloud import bigquery
from google.cloud.bigquery import job as bigquery_job
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest
from src.logger import Logger
from src.exceptions import BigQueryErrors


def json_serial(obj):
    """Serialiser datoobjekter for JSON.

    Parameters
    ----------
    obj : Any
        Objektet som skal serialiseres.

    Returns
    -------
    str
        Dato eller klokkeslett på ISO 8601-format.

    Raises
    ------
    TypeError
        Hvis ``obj`` ikke er en ``datetime``- eller ``date``-instans.
    """

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class BQConnector:
    """Kobler til BigQuery og utfører spørringer og tabelloperasjoner.

    Attributes
    ----------
    client : google.cloud.bigquery.Client
        BigQuery-klienten som brukes til API-kall.
    """

    def __init__(self):
        """Opprett en BigQuery-klient for denne tilkoblingen."""
        self.client: bigquery.Client = self._create_client()

    def _execute_query(self, query: str) -> bigquery.QueryJob:
        """Kjør en SQL-spørring mot BigQuery.

        Parameters
        ----------
        query : str
            SQL-spørringen som skal kjøres.

        Returns
        -------
        google.cloud.bigquery.QueryJob
            Jobben som representerer spørringen.
        """
        return self.client.query(query=query)

    def query(self, query: str):
        """Kjør en spørring og vent på at BigQuery-jobben blir ferdig.

        Parameters
        ----------
        query : str
            SQL-spørringen som skal kjøres.

        Raises
        ------
        BigQueryMissingRequiredError, BigQueryInvalidValueError, BigQueryError
            En klassifisert feil dersom jobben feiler med ``BadRequest``
            og feilen er representert i jobbens feilliste.
        """
        query_job = self._execute_query(query=query)

        try:
            query_job.result()
        except BadRequest:
            bq_errors = BigQueryErrors(errors=query_job.errors)
            for exception in bq_errors:
                raise exception

    def get_rows(self, query: str) -> List[Dict[str, Any]]:
        """Kjør en spørring og returner radene som ordbøker.

        Parameters
        ----------
        query : str
            SQL-spørringen som skal kjøres.

        Returns
        -------
        list of dict of str to Any
            Rader med kolonnenavn som nøkler og kolonneverdier som verdier.
        """
        query_job = self._execute_query(query=query)
        return self._format_results(query_job=query_job)

    def put_rows(
        self, rows: list, table_id: str, job_config: bigquery.LoadJobConfig
    ) -> None:
        """Last JSON-rader opp til en BigQuery-tabell og vent på jobben.

        Parameters
        ----------
        rows : list
            Radene som skal lastes opp.
        table_id : str
            Identifikatoren til måltabellen.
        job_config : google.cloud.bigquery.LoadJobConfig
            Konfigurasjon for lastejobben.

        Raises
        ------
        BigQueryMissingRequiredError, BigQueryInvalidValueError, BigQueryError
            En klassifisert feil dersom jobben feiler med ``BadRequest``
            og feilen er representert i jobbens feilliste.
        """
        job = self.client.load_table_from_json(
            json_rows=rows, destination=table_id, job_config=job_config
        )

        try:
            job.result()
        except BadRequest:
            bq_errors = BigQueryErrors(errors=job.errors)
            for exception in bq_errors:
                raise exception

    def put_rows_alt(
        self,
        rows: list[dict[str, Any]],
        table_id: str,
        job_config: bigquery.LoadJobConfig,
    ):
        """Last rader opp som linjedelt JSON med egen dato-serialisering.

        Radene serialiseres én per linje til en bytes-buffer og lastes opp med
        ``load_table_from_file``.

        Parameters
        ----------
        rows : list of dict
            Radene som skal serialiseres og lastes opp.
        table_id : str
            Identifikatoren til måltabellen.
        job_config : google.cloud.bigquery.LoadJobConfig
            Konfigurasjon for lastejobben; kildeformatet settes til linjedelt
            JSON.

        Raises
        ------
        BigQueryMissingRequiredError, BigQueryInvalidValueError, BigQueryError
            En klassifisert feil dersom jobben feiler med ``BadRequest``
            og feilen er representert i jobbens feilliste.
        """
        data_str = "\n".join(
            json.dumps(item, ensure_ascii=False, default=json_serial) for item in rows
        )
        encoded_str = data_str.encode()
        data_file = io.BytesIO(encoded_str)
        job_config.source_format = bigquery_job.SourceFormat.NEWLINE_DELIMITED_JSON

        job = self.client.load_table_from_file(
            file_obj=data_file,
            size=len(encoded_str),
            destination=table_id,
            job_config=job_config,
        )

        try:
            job.result()
        except BadRequest:
            bq_errors = BigQueryErrors(errors=job.errors)
            for exception in bq_errors:
                raise exception

    @staticmethod
    def _create_client() -> bigquery.Client:
        """Opprett en BigQuery-klient med klientbibliotekets standardoppsett.

        Returns
        -------
        google.cloud.bigquery.Client
            Den opprettede BigQuery-klienten.
        """
        return bigquery.Client()

    @staticmethod
    def _format_results(query_job: bigquery.QueryJob) -> List[Dict[str, Any]]:
        """Formater resultatene fra en BigQuery-jobb som ordbøker.

        Parameters
        ----------
        query_job : google.cloud.bigquery.QueryJob
            Jobben med spørringsresultatene.

        Returns
        -------
        list of dict of str to Any
            Resultatrader med kolonnenavn som nøkler.
        """
        results = query_job.result()
        return [{key: value for key, value in row.items()} for row in results]

    def create_dataset(
        self,
        dataset_name: str,
        logger: Logger,
        description: str = None,
        location: str = "europe-north1",
    ) -> None:
        """Opprett et datasett med valgfri beskrivelse og plassering.

        Parameters
        ----------
        dataset_name : str
            Navnet eller referansen til datasettet.
        logger : Logger
            Logger som får melding før opprettelsen.
        description : str, optional
            Beskrivelse som settes dersom verdien er sann.
        location : str, default "europe-north1"
            Plasseringen som settes på datasettet.
        """

        dataset = bigquery.Dataset(dataset_ref=dataset_name)
        dataset.location = location

        if description:
            dataset.description = description

        logger.info(
            f"Creating dataset {dataset_name} in {dataset.project} in {dataset.location}"
        )
        self.client.create_dataset(dataset)

    def delete_table(self, table_name: str, dataset: str, logger: Logger) -> None:
        """Slett en tabell i det aktive BigQuery-prosjektet.

        Parameters
        ----------
        table_name : str
            Navnet på tabellen som skal slettes.
        dataset : str
            Datasettet tabellen tilhører.
        logger : Logger
            Logger som får melding før slettingen.
        """
        project_id = self.client.project
        table_id = f"{project_id}.{dataset}.{table_name}"
        logger.info(f"Deleting table {table_id} in {dataset} in {project_id}")
        self.client.delete_table(table_id)

    def check_if_table_exists_in_bq(self, table_id: str) -> bool:
        """Sjekk om en BigQuery-tabell finnes.

        Parameters
        ----------
        table_id : str
            Tabellens identifikator.

        Returns
        -------
        bool
            ``True`` hvis tabellen finnes, ellers ``False``.
        """
        try:
            self.client.get_table(table_id)  # Make an API request.
            table_exists_in_bq = True
        except NotFound:
            table_exists_in_bq = False
        return table_exists_in_bq

    def update_table_and_col_descriptions(
        self, table_id: str, desc: str, schema: List, logger: Logger
    ):
        """Oppdater tabellbeskrivelse og skjema når verdiene er endret.

        Parameters
        ----------
        table_id : str
            Tabellens identifikator.
        desc : str
            Ny tabellbeskrivelse.
        schema : list
            Nytt tabellskjema.
        logger : Logger
            Logger som får meldinger om kontroll og eventuell oppdatering.
        """
        logger.info(f"Sjekker tabell {table_id} for oppdateringer")

        bq_table = self.client.get_table(table_id)

        # oppdaterer tabellen
        original_desc = bq_table.description
        original_schema = bq_table.schema
        if (desc != original_desc) or (schema != original_schema):
            logger.info(
                f"Oppdaterer beskrivelse og /eller schema for tabell {table_id}"
            )
            bq_table.description = desc
            bq_table.schema = schema
            self.client.update_table(bq_table, ["description", "schema"])
