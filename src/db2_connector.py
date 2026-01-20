import os
import ibm_db


from typing import List, Dict, Any, Iterator
from pandas import DataFrame


class DB2Connector:

    def __init__(
        self, database_name: str, host: str, port: str, username: str, password: str
    ):
        self._database_name = database_name
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self.connection: ibm_db.IBM_DBConnection = self._create_connection()

    def exec_immediate(self, query):
        ibm_db.exec_immediate(self.connection, query)

    def close(self):
        ibm_db.close(self.connection)

    def get_chunks(
        self, chunk_size: int, base_query: str, binds: dict
    ) -> Iterator[list[dict[str, Any]]]:

        offset = 0

        done = False
        while not done:
            query = f"""{base_query} OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"""

            rows = self.get_rows(query=query, binds=binds)
            offset += chunk_size

            if len(rows) < chunk_size:
                done = True

            yield rows

    def get_rows(self, query: str, binds: Dict[int, Any]) -> List[Dict[str, Any]]:

        statement = ibm_db.prepare(self.connection, query)
        if not binds:
            binds = {}

        for _index, value in binds.items():
            ibm_db.bind_param(
                statement, _index, value, ibm_db.SQL_PARAM_INPUT, ibm_db.SQL_CHAR
            )

        ibm_db.execute(statement)

        rows = []

        current_row: Dict[str, Any] = ibm_db.fetch_assoc(statement)

        while current_row:
            rows.append(current_row)
            current_row = ibm_db.fetch_assoc(statement)

        return rows

    def get_rows_as_dataframe(
        self, query: str, binds: Dict[int, Any] = None
    ) -> DataFrame:

        rows = self.get_rows(query=query, binds=binds)
        return DataFrame(data=rows)

    def _create_connection(self) -> ibm_db.IBM_DBConnection:
        dsn = self._create_dsn()
        connection = ibm_db.connect(dsn, "", "")
        return connection

    def _create_dsn(self):
        dsn = (
            f"DRIVER={{IBM DB2 ODBC DRIVER}};"
            f"DATABASE={self._database_name};"
            f"HOSTNAME={self._host};"
            f"PORT={self._port};"
            f"PROTOCOL=TCPIP;"
            f"UID={self._username};"
            f"PWD={self._password};"
        )

        return dsn

    @staticmethod
    def create_connector_from_envs() -> "DB2Connector":
        database_name = os.environ["DATABASE_NAME"]
        username = os.environ["DATABASE_USERNAME"]
        password = os.environ["DATABASE_PASSWORD"]
        port = os.environ["DATABASE_PORT"]
        host = os.environ["DATABASE_HOST"]

        conn = DB2Connector(
            database_name=database_name,
            host=host,
            port=port,
            username=username,
            password=password,
        )

        if conn._database_name == "TDB2":
            conn.exec_immediate("SET CURRENT QUERY ACCELERATION ALL")

        return conn
