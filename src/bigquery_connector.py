from typing import List, Dict, Any
from datetime import datetime, date

import json
import io

from google.cloud import bigquery
from google.cloud.bigquery import job as bigquery_job
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest
from pandas import DataFrame
from src.logger import Logger
from src.exceptions import BigQueryErrors


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))

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


    def query(self, query: str):
        query_job = self._execute_query(query=query)

        try:
            query_job.result()
        except BadRequest:
            bq_errors = BigQueryErrors(errors=query_job.errors)
            for exception in bq_errors:
                raise exception

    def get_rows(self, query: str) -> List[Dict[str, Any]]:
        """
        Metode for å kjøre spørring med resultater og som skal formaters som en liste av dicts.
        Args:
            query (str): SQL spørring som skal kjøres

        Returns List[Dict[str, Any]: En liste av dicts med keys som kolonnenavn og values som verdi til kolonne.

        """
        query_job = self._execute_query(query=query)
        return self._format_results(query_job=query_job)

    def put_rows(
        self, rows: list, table_id: str, job_config: bigquery.LoadJobConfig
    ) -> None:
        job = self.client.load_table_from_json(
            json_rows=rows, destination=table_id, job_config=job_config
        )

        try:
            job.result()
        except BadRequest:
            bq_errors = BigQueryErrors(errors=job.errors)
            for exception in bq_errors:
                raise exception

    def put_rows_alt(self, rows: list[dict[str, Any]], table_id: str, job_config: bigquery.LoadJobConfig):
        data_str = "\n".join(json.dumps(item, ensure_ascii=False,default=json_serial) for item in rows)
        encoded_str = data_str.encode()
        data_file = io.BytesIO(encoded_str)
        job_config.source_format =  bigquery_job.SourceFormat.NEWLINE_DELIMITED_JSON

        job = self.client.load_table_from_file(file_obj=data_file,
                                               size=len(encoded_str),
                                               destination=table_id,
                                               job_config=job_config)

        try:
            job.result()
        except BadRequest:
            bq_errors = BigQueryErrors(errors=job.errors)
            for exception in bq_errors:
                raise exception

    def put_dataframe(
        self, df: DataFrame, table_id: str, job_config: bigquery.LoadJobConfig
    ) -> None:
        job = self.client.load_table_from_dataframe(
            dataframe=df, destination=table_id, job_config=job_config
        )

        try:
            job.result()
        except BadRequest:
            bq_errors = BigQueryErrors(errors=job.errors)
            for exception in bq_errors:
                raise exception

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
    def _format_results(query_job: bigquery.QueryJob) -> List[Dict[str, Any]]:
        """
        Statisk metode for å formatere resultater fra en QueryJob.
        Args:
            query_job (bigquery.QueryJob): QueryJob med resultater fra en spørring

        Returns:

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

        dataset = bigquery.Dataset(dataset_ref=dataset_name)
        dataset.location = location

        if description:
            dataset.description = description

        logger.info(
            f"Creating dataset {dataset_name} in {dataset.project} in {dataset.location}"
        )
        self.client.create_dataset(dataset)

    def delete_table(self, table_name: str, dataset: str, logger: Logger) -> None:
        project_id = self.client.project
        table_id = f"{project_id}.{dataset}.{table_name}"
        logger.info(f"Deleting table {table_id} in {dataset} in {project_id}")
        self.client.delete_table(table_id)

    def check_if_table_exists_in_bq(self, table_id: str) -> bool:
        # Check if table exists in BQ, and set appropriate load method.
        try:
            self.client.get_table(table_id)  # Make an API request.
            table_exists_in_bq = True
        except NotFound:
            table_exists_in_bq = False
        return table_exists_in_bq

    def update_table_and_col_descriptions(
        self, table_id: str, desc: str, schema: List, logger: Logger
    ):
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
