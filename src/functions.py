from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from typing import Union, Any
from yaml import safe_load

from src.bigquery_connector import BQConnector
from src.db2_connector import DB2Connector
from src.class_table import DimTable, FakTable, TableType
from src.logger import Logger
from src.env_handler import EnvHandler
from src.config_loader import TableModel


def set_and_check_envs(source_name: str) -> None:
    """Last inn og kontroller miljøvariablene for en datakilde.

    Parameters
    ----------
    source_name : str
        Navnet på datakilden som miljøvariablene gjelder for.

    Returns
    -------
    None
        Funksjonen returnerer ikke en verdi.

    Raises
    ------
    EnvsNotSetError
        Hvis en påkrevd miljøvariabel mangler.
    Db2LicenseNotFoundError
        Hvis lisensfilen mangler ved kjøring utenfor lokalt miljø.
    """
    env_handler = EnvHandler(source_name=source_name)
    env_handler.load_envs()
    env_handler.check_envs()


def load_config_tables(
    config_path: str,
) -> list[Union[DimTable, FakTable]]:
    """Les YAML-konfigurasjonen og bygg tabellobjekter.

    Parameters
    ----------
    config_path : str
        Filsti til YAML-filen med en ``tables``-liste.

    Returns
    -------
    list of DimTable or FakTable
        Tabellobjektene som er bygget fra konfigurasjonen.

    Raises
    ------
    FileNotFoundError
        Hvis konfigurasjonsfilen ikke finnes.
    KeyError
        Hvis YAML-innholdet mangler nøkkelen ``tables``.
    """
    with open(config_path) as file:
        tables = safe_load(file)

    table_objs = []
    for table in tables["tables"]:
        table_objs.append(TableModel.from_dict(table).to_table_object())

    return table_objs


def get_from_datetime(
    bq_client: BQConnector, table: Union[DimTable, FakTable], table_exists_in_bq: bool
) -> str:
    """Finn starttidspunktet for innlasting og formater det som tekst.

    Når tabellen finnes i BigQuery, brukes maksimumsverdien i tabellens
    kontrollkolonne. Ellers brukes tidspunktet 730 dager før dagens dato.

    Parameters
    ----------
    bq_client : BQConnector
        Klient som utfører spørringen mot BigQuery.
    table : DimTable or FakTable
        Tabell med måltabell-ID og kontrollkolonne.
    table_exists_in_bq : bool
        Angir om måltabellen allerede finnes.

    Returns
    -------
    str
        Tidspunkt formatert som ``YYYY-MM-DD HH:MM:SS.ffffff``.

    Raises
    ------
    IndexError
        Hvis oppslaget mot en eksisterende tabell ikke gir noen rader.
    AttributeError
        Hvis maksimumsverdien er ``None`` og dermed ikke har ``strftime``.
    """
    if table_exists_in_bq:
        max_query = f"SELECT MAX({table.check_col}) FROM {table.bq_table_id}"
        from_date_dict: dict[str, Any] = bq_client.get_rows(query=max_query)[0]
        from_date = list(from_date_dict.values())[0]

    else:
        from_date = datetime.today() - timedelta(days=730)

    return from_date.strftime("%Y-%m-%d %H:%M:%S.%f")


def generate_limits(start_datetime: datetime) -> list[date]:
    """Lag månedlige dato-grenser fra startmåneden til neste måned.

    Parameters
    ----------
    start_datetime : datetime
        Starttidspunkt; dagen ignoreres og erstattes med første dag i måneden.

    Returns
    -------
    list of date
        Første dag i hver måned, inkludert måneden som inneholder dagens dato
        og den påfølgende måneden.
    """
    start_date = start_datetime.date().replace(day=1)
    end_date = datetime.today().date() + relativedelta(months=1)

    date_list = []

    current = start_date
    while current <= end_date:
        date_list.append(current)
        current += relativedelta(months=1)

    return date_list


def delete_table(
    table: Union[DimTable, FakTable], bq_client: BQConnector, logger: Logger
) -> None:
    """Slett tabellen fra BigQuery.

    Parameters
    ----------
    table : DimTable or FakTable
        Tabellobjekt som angir tabell-ID og datasett.
    bq_client : BQConnector
        Klient som utfører slettingen.
    logger : Logger
        Logger som brukes av klienten under slettingen.

    Returns
    -------
    None
        Funksjonen returnerer ikke en verdi.
    """
    table_name = table.bq_table_id
    table_dataset = table.bq_dataset
    bq_client.delete_table(table_name=table_name, dataset=table_dataset, logger=logger)


def create_datasets(
    datasets: list[str], bq_connector: BQConnector, logger: Logger
) -> None:
    """Opprett hvert datasett i listen gjennom BigQuery-klienten.

    Parameters
    ----------
    datasets : list of str
        Datasettnavn som skal opprettes.
    bq_connector : BQConnector
        Klient som oppretter datasettene.
    logger : Logger
        Logger som brukes av klienten.

    Returns
    -------
    None
        Funksjonen returnerer ikke en verdi.
    """
    for dataset in datasets:
        bq_connector.create_dataset(dataset, logger=logger)


def db2_to_bq(
    table: Union[DimTable, FakTable],
    bq_client: BQConnector,
    db2_conn: DB2Connector,
    logger: Logger,
):
    """Hent tabellrader fra DB2 i bolker og last dem til BigQuery.

    For faktatabeller kontrolleres måltabellen og ``from_datetime`` beregnes
    før SQL-spørringen bygges. For dimensjonstabeller utføres ikke denne
    kontrollen her; SQL og bind-verdier hentes fra tabellobjektet.

    Parameters
    ----------
    table : DimTable or FakTable
        Tabellbeskrivelse som bygger DB2-spørring og BigQuery-konfigurasjon.
    bq_client : BQConnector
        Klient som kontrollerer måltabellen og laster radbolker.
    db2_conn : DB2Connector
        DB2-tilkobling som produserer radbolker.
    logger : Logger
        Logger for statusmeldinger.

    Returns
    -------
    None
        Funksjonen returnerer ikke en verdi.
    """
    logger.info(
        f"Processing table: {table.name.upper()} of type:{table.table_type.value.upper()}"
    )

    if table.table_type == TableType.FAK:
        table_exists_in_bq = bq_client.check_if_table_exists_in_bq(
            table_id=table.bq_table_id
        )
        logger.info(f"{table.name.upper()} exists: {table_exists_in_bq}")
        table.from_datetime = get_from_datetime(
            bq_client=bq_client, table=table, table_exists_in_bq=table_exists_in_bq
        )

    base_query = table.build_sql_db2()
    binds = table.generate_binds()
    job_config = table.make_bq_load_job_config()

    chunk_size = 1000000
    total_rows = 0

    for chunk in db2_conn.get_chunks(
        chunk_size=chunk_size, base_query=base_query, binds=binds
    ):
        if len(chunk) > 0:
            bq_client.put_rows_alt(
                chunk, table_id=table.bq_table_id, job_config=job_config
            )
        total_rows += len(chunk)
        logger.info(
            f"Total rows: {total_rows} and chunk of size: {len(chunk)} rows was written to {table.name.upper()}"
        )


def update_desc(logger: Logger, source_name: str):
    """Oppdater beskrivelser og skjema for tabellene i BigQuery.

    Laster miljøvariabler og tabellkonfigurasjon, og sender beskrivelse og
    kolonnedefinisjoner for hver tabell til BigQuery-klienten.

    Parameters
    ----------
    logger : Logger
        Logger for statusmeldinger og klientoperasjoner.
    source_name : str
        Navnet på datakilden som miljøvariablene gjelder for.

    Returns
    -------
    None
        Funksjonen returnerer ikke en verdi.

    Notes
    -----
    Konfigurasjonsstien settes til ``{source_name}_tables.yaml`` relativt til
    gjeldende arbeidskatalog.
    """
    set_and_check_envs(source_name=source_name)

    tables = load_config_tables(config_path=f"{source_name}_tables.yaml")

    logger.info("Oppdater beskrivelse og schema i alle BigQuery-tabellene")

    bq_client = BQConnector()
    for table in tables:
        bq_client.update_table_and_col_descriptions(
            table_id=table.bq_table_id,
            desc=table.description,
            schema=table.cols,
            logger=logger,
        )
