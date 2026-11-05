import os
import ibm_db


from typing import List, Dict, Any, Iterator


class DB2Connector:
    """Opprettholder en tilkobling til DB2 og utfører spørringer.

    Attributes
    ----------
    connection : ibm_db.IBM_DBConnection
        DB2-tilkoblingen som brukes av operasjonene.
    """

    def __init__(
        self, database_name: str, host: str, port: str, username: str, password: str
    ):
        """Opprett en DB2-tilkobling fra oppgitte tilkoblingsparametere.

        Parameters
        ----------
        database_name : str
            Navnet på databasen.
        host : str
            DB2-serverens host.
        port : str
            Serverens port.
        username : str
            Brukernavnet for tilkoblingen.
        password : str
            Passordet for tilkoblingen.
        """
        self._database_name = database_name
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self.connection: ibm_db.IBM_DBConnection = self._create_connection()

    def exec_immediate(self, query):
        """Utfør en SQL-setning direkte på den aktive DB2-tilkoblingen.

        Parameters
        ----------
        query : str
            SQL-setningen som skal utføres.
        """
        ibm_db.exec_immediate(self.connection, query)

    def close(self):
        """Lukk den aktive DB2-tilkoblingen."""
        ibm_db.close(self.connection)

    def get_chunks(
        self, chunk_size: int, base_query: str, binds: dict
    ) -> Iterator[list[dict[str, Any]]]:
        """Hent spørringsresultatet i forskjøvede porsjoner.

        Parameters
        ----------
        chunk_size : int
            Maksimalt antall rader per porsjon.
        base_query : str
            Grunnspørringen, som får ``OFFSET`` og ``FETCH NEXT``-ledd.
        binds : dict
            Parameterbindinger som sendes videre til ``get_rows``.

        Yields
        ------
        list of dict of str to Any
            En porsjon med rader; en siste porsjon kan være tom.
        """

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
        """Kjør en forberedt DB2-spørring og hent alle radene.

        Parameters
        ----------
        query : str
            SQL-spørringen som skal forberedes og kjøres.
        binds : dict of int to Any
            Bindingsposisjoner og verdier for spørringen.

        Returns
        -------
        list of dict of str to Any
            Hentede rader med kolonnenavn som nøkler.
        """

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

    def _create_connection(self) -> ibm_db.IBM_DBConnection:
        """Opprett DB2-tilkoblingen fra en DSN bygget av lagrede innstillinger.

        Returns
        -------
        ibm_db.IBM_DBConnection
            Den opprettede DB2-tilkoblingen.
        """
        dsn = self._create_dsn()
        connection = ibm_db.connect(dsn, "", "")
        return connection

    def _create_dsn(self):
        """Bygg en ODBC-tilkoblingsstreng fra lagrede DB2-innstillinger.

        Returns
        -------
        str
            DSN-strengen for tilkoblingen.
        """
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
        """Opprett en DB2-tilkobling med innstillinger fra miljøvariabler.

        For databasen ``TDB2`` kjøres også en setning for
        spørringsakselerasjon etter tilkoblingen.

        Returns
        -------
        DB2Connector
            Den opprettede DB2-tilkoblingen.

        Raises
        ------
        KeyError
            Hvis en påkrevd miljøvariabel mangler.
        """
        database_name = os.environ["DATABASE_NAME"]
        username = os.environ[f"DATABASE_USERNAME"]
        password = os.environ[f"DATABASE_PASSWORD"]
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
            conn.exec_immediate("SET CURRENT QUERY ACCELERATION ELIGIBLE")

        return conn
