import json

import pandas as pd

# import polars as pl
# import duckdb as dd
from sqlalchemy import create_engine

import ibm_db

import time
import os


# from google.cloud.functions import get_gsm_secret

from google.cloud import bigquery


def connect_db2(lokal=True):
    if lokal:
        with open(
            "//Users//Linda.Josephine.Claesson//Documents//creds_q2_lcc.json"
        ) as json_file:
            creds = json.load(json_file)
    else:
        # creds = get_gsm_secret(
        #    project_id="utsikt-dev-3609", secret_name="oppdrag-q1-credentials"
        # )
        with open(
            "//Users//Linda.Josephine.Claesson//Documents//creds_q2_lcc.json"
        ) as json_file:
            creds = json.load(json_file)

    database_username = creds.get("DATABASE_USERNAME")
    database_password = creds.get("DATABASE_PASSWORD")
    database_hostname = creds.get("DATABASE_HOSTNAME")
    database_port = creds.get("DATABASE_PORT")
    database_name = creds.get("DATABASE_NAME")

    # Construct the connection string
    connection_string = (
        f"DATABASE={database_name};"
        f"HOSTNAME={database_hostname};"
        f"PORT={database_port};"
        f"PROTOCOL=TCPIP;"
        f"UID={database_username};"
        f"PWD={database_password};"
    )

    # Establish the connection
    db2_connection_string = f"db2+ibm_db://{database_username}:{database_password}@{database_hostname}:{database_port}/{database_name}"

    engine = create_engine(db2_connection_string)
    connection = engine.connect()

    return connection


def fetch_data_db2(query=None, lokal=True):

    connection = connect_db2(lokal)

    if query:
        df = pd.read_sql(query, connection)
        # df = pl.read_database(query, connection)
        print(f"hentet {len(df)} rader")
        return df

    else:
        raise ValueError


def create_db2_conn(local_dev: bool = False):
    from dotenv import load_dotenv

    load_dotenv()

    database_username = os.environ.get("DATABASE_USERNAME")
    database_password = os.environ.get("DATABASE_PASSWORD")
    database_host = os.environ.get("DATABASE_HOST", default="155.55.1.82")
    database_port = os.environ.get("DATABASE_PORT", default="5025")
    database_name = os.environ.get("DATABASE_NAME", default="QDB2")

    dsn = (
        f"DRIVER={{IBM DB2 ODBC DRIVER}};"
        f"DATABASE={database_name};"
        f"HOSTNAME={database_host};"
        f"PORT={database_port};"
        f"PROTOCOL=TCPIP;"
        f"UID={database_username};"
        f"PWD={database_password};"
    )
    # Establish the connection
    print("Attempting to connect to database.")
    try:
        db2_conn = ibm_db.connect(dsn, "", "")
        print("Connected to the database!")
    except Exception as e:
        print(e)
        exit(1)
    return db2_conn


def fetch_data_db2_using_ibm_db(query=None, lokal=True):

    connection = create_db2_conn(lokal)

    stmt = ibm_db.exec_immediate(connection, query)
    rows = []
    row = ibm_db.fetch_assoc(stmt)
    while row:
        rows.append(row)
        row = ibm_db.fetch_assoc(stmt)

    df = pd.DataFrame(rows)
    print(f"Hentet {len(df)} rader fra db2")

    return df


query_stoppstatus_cnt = f"""
select
count(*)
from OS231Q2.T_VENT_STOPPSTATUS /* oversikt over beregningsflyten og identifisere eventuelle flaskehalser */
"""
# df_stoppstatus_cnt = fetch_data_db2(query_stoppstatus_cnt,True)
# print(df_stoppstatus_cnt)

# Using pandas to only fetch number of rows
# Q2:     1 098 972 , tok noen sekunder
# Q1: 1 580 101 119 , tok litt over ett minutt

# Using polars to only fetch number of rows
# Q2:     1 101 422 , par sekunder
# Q1: 1 580 101 273 , tok ca 1 min 10 s

query_stoppstatus = f"""
select
beregnings_id /* kobling */
,stoppnivaa_id /* kobling */
,kode_ventestatus /* id for ventestatus */
,Lopenr /* tallet er >=1, hopper med steg += 1, 9999 for gjeldendeventestatus */
,Tidspkt_reg /* tidspunktet ventestatusen er registert. Når siste statusrad settes til lopenr = 9999, så oppdateres også statusen på tidligere rad som hadde gjeldende ventestatus, men oppdatere ikke tidspkt_reg for den tidligere gjeldende statusraden (i.e. tidspkt_reg har kun insert logikk). */
from OS231Q1.T_VENT_STOPPSTATUS /* oversikt over beregningsflyten og identifisere eventuelle flaskehalser */
fetch first 1000000 rows only
"""

# df_stoppstatus = fetch_data_db2(query_stoppstatus,False)

query_stoppstatuskode = f"""
select
Kode_ventestatus /* id for ventestatus */
,Beskrivelse /* beskrivelsen av ventestatus, for vanskelig å vite hva ventestatusen betyr uten beskrivelsen */
from OS231Q2.T_VENT_STATUSKODE /* betydning av ventestatuskoden */
"""
# df_statuskode = fetch_data_db2(query_stoppstatuskode)

# Bruke pandas for å hente alle rader med utvalgte kolonner
# Q2: 1101422, ca 13 s
# Q1: - stoppet manuelt over 15 min senere.

# Bruke polars for å hente alle rader med utvalgte kolonner
# Q2: 1101422, ca 14 s
# Q1 - stoppet manuelt 12 min senere

# Hvis det tar like lang tid med pd vs pl, og pl er foventet å ta kortere tid, kan det være noe med at jeg har brukt sqlalchemy for connect i begge tilfeller? Er det der delay'en er?

# Prøve duckdb
# Finner ingen måte å kunne bruke duckdb til å hente fra db2. Men kan nok brukes i senere steg.

# Prøve pandas/polars/duckdb rett inn i bq vs mellomlagre i parquet (insert)

# bq_client = bigquery.Client(project='utsikt-dev-3609')


# job_config = bigquery.LoadJobConfig(
#     write_disposition = "WRITE_TRUNCATE",
#     create_disposition="CREATE_IF_NEEDED",
# )

# table_id = "venteregister.testing"

# job = bq_client.load_table_from_dataframe(
#     df_statuskode, table_id, job_config=job_config
# )  # Make an API request.
# job.result()  # Wait for the job to complete.


# load_tale_from_dataframe is tranformed into parquet before insert, so in gcp it reports that the source format is parquet. Er det da noe grunn til å gjøre om til parquet FØR insert i bq?

# ttrenger et mellomstadiet (typ det jeg ville kalt stagig) eller temporary tables, der vi kanskje kun henter de nyeste dataene.
# Hvordan ville dette sett ut? Skulle vi brukt insert into bq, og så hva da for insert into bq?
# Kanskje for vent_stoppstatus så hente alle stoppstatusene der et tidspkt_reg > enn det som allerede er registrert i hovedtabellen i bq


# sjekket om det var forkjsll på å hentet like mange antall rader fra q2 vs q1, ingen forkjell.

# Prøve pandas/polars/duckdb rett inn i bq vs mellomlagre i parquet (update)


# Kanskje neste steg er bare å utforske noe annet enn sqlalchemy for hastighet? Og sette opp innlasting av ulike tabeller, noe med delta og andre bare direkte innhenting


##################################
start_time = time.time()

query_stoppstatus_cnt = f"""
select
count(*)
from OS231Q2.T_VENT_STOPPSTATUS
"""
# df_stoppstatus_cnt = fetch_data_db2(query_stoppstatus_cnt, True)               # --- 0.241 seconds --- --- 0.114 seconds --- --- 0.134 seconds ---
# df_stoppstatus_cnt = fetch_data_db2_using_ibm_db(query_stoppstatus_cnt, True)  # --- 0.110 seconds --- --- 0.099 seconds --- --- 0.101 seconds ---
print("--- %.3f seconds ---" % (time.time() - start_time))

# Virker som at ibm_db er raskere enn sqlalchemy.

# ibm_db er den grunnleggende pakken for å koble seg til db2. SQLAlchemy kan også brukes til å koble seg til, men er i hovedsak praktisk i det at man kan bruke SQLAlchemy for å koble seg til andre typer databaser, og den tillater deg å skrive enklere python-kode (https://www.cdata.com/blog/what-is-sqlalchemy)
# Som vi ser det nå (sep 2025) så bruker vi heller ibm_db for så lav kompleksitet som mulig, da vi per nå ikke ser behov for disse fordelene med SQLAlchemy.

# Skal og være mulig å koble seg til db2 ved å bruke pyodbc, som er en generell pakke for å koble seg til mange ulike typer databaser (ikke bare ibm db2). Da trenger man en i tillegg en ODBC driver for db2.
# Ibm_db er spesifikk for IBM Db2, og kommer med Python driver for IBM Db2. Dermed sikkert også raskere enn pyodbc.
# Kunne sikkert teste hastigheten med pyodbc, men har ikke nødvendigvis tro på at det vil være et bedre alnternativ for oss enn ibm_db.
