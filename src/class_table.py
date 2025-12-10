import os
from datetime import datetime
from typing import Any
from enum import Enum
from abc import ABC, abstractmethod

from google.cloud.bigquery import SchemaField, LoadJobConfig, table


class TableType(Enum):
    DIM = "dim"
    FAK = "fak"


class BaseTable(ABC):

    def __init__(self, name: str, description: str, cols: list[SchemaField], table_type: TableType) -> None:
        self._name = name
        self._description = description
        self._cols = cols
        self._table_type = table_type
        self._db2_schema = None
        self._bq_dataset = None
        self._bq_table_id = None

        self._set_envs()


    def _set_envs(self) -> None:
        self._db2_schema: str = os.environ["DATABASE_SCHEMA"]
        self._bq_dataset: str = self._db2_schema[:2] + "_" + self._db2_schema[-2:]
        self._bq_table_id = f"{self._bq_dataset}.{self._name}"


    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def cols(self) -> list[SchemaField]:
        return self._cols

    @property
    def db2_schema(self) -> str:
        return self._db2_schema

    @property
    def bq_dataset(self) -> str:
        return self._bq_dataset

    @property
    def bq_table_id(self) -> str:
        return self._bq_table_id

    @property
    def table_type(self) -> TableType:
        return self._table_type

    @abstractmethod
    def build_sql_db2(self) -> str:
        raise NotImplementedError()

    def _build_sql_db2(self) -> str:
        column_names: str = ",".join([col.name for col in self.cols])
        query = f"""SELECT {column_names} FROM {self.db2_schema}.{self.name} """
        return query

    @abstractmethod
    def make_bq_load_job_config(self) -> LoadJobConfig:
        raise NotImplementedError()

    def _make_bq_load_job_config(self, write_disposition: str) -> LoadJobConfig:
        schema = self.cols
        create_disposition = "CREATE_IF_NEEDED"

        job_config = LoadJobConfig(schema=schema,
                                   write_disposition=write_disposition,
                                   create_disposition=create_disposition)

        return job_config

    @abstractmethod
    def generate_binds(self) -> dict[int, Any]:
        raise NotImplementedError()

    @staticmethod
    def _generate_binds() -> dict[int, Any]:
        return {}



class DimTable(BaseTable):
    def __init__(self, name: str, description: str, cols: list[SchemaField]) -> None:
        super().__init__(name=name, description=description, cols=cols, table_type=TableType.DIM)

    def build_sql_db2(self) -> str:
        return self._build_sql_db2()

    def make_bq_load_job_config(self) -> LoadJobConfig:
        return self._make_bq_load_job_config(write_disposition="WRITE_TRUNCATE")

    def generate_binds(self) -> dict[int, Any]:
        return self._generate_binds()


class FakTable(BaseTable):
    def __init__(self, name: str, description: str, cols: list[SchemaField], check_col: str) -> None:
        super().__init__(name=name, description=description, cols=cols, table_type=TableType.FAK)
        self._check_col = check_col
        self._from_datetime = None



    @property
    def check_col(self) -> str:
        return self._check_col

    @property
    def from_datetime(self) -> datetime:
        return self._from_datetime

    @from_datetime.setter
    def from_datetime(self, from_datetime: datetime) -> None:
        self._from_datetime = from_datetime


    def build_sql_db2(self) -> str:
        base_query = self._build_sql_db2()
        col_query = f"WHERE {self._check_col} > ?"

        return base_query + col_query

    def make_bq_load_job_config(self) -> LoadJobConfig:
        base_job_config = self._make_bq_load_job_config(write_disposition="WRITE_APPEND")
        _field: str = self.check_col

        partition = table.TimePartitioning(type_="DAY",
                                           field=_field,
                                           expiration_ms=1000 * 60 * 60 * 24 * 730)

        base_job_config.time_partitioning = partition
        return base_job_config


    def generate_binds(self) -> dict[int, Any]:
        return {1: self.from_datetime}
