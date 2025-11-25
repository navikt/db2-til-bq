import os
import ibm_db
import pandas as pd
from src.class_table import Table


def build_sql_init(self, schema: str, offset: int, chunk_size: int) -> str:
    query = f"""SELECT {', '.join(self.columns)} 
                FROM {schema}.{self.name}
                OFFSET {offset} ROWS 
                FETCH NEXT {chunk_size} ROWS ONLY
                """
    return query


def fetch_data_chunk(db2_conn, query):
    """
    Fetches a chunk of data from the database table.

    Returns:
        list: A list of dictionaries, each representing a row.
    """

    stmt = ibm_db.exec_immediate(db2_conn, query)
    rows = []
    row = ibm_db.fetch_assoc(stmt)
    while row:
        rows.append(row)
        row = ibm_db.fetch_assoc(stmt)

    return rows


def fetching_table(conn, db_table: Table, chunk_size: int = 100000):
    """
    Fetches data from a specified IBM DB2 source table in chunks and writes each chunk to GCP BigQuery.
    """

    try:
        offset = 0
        while True:
            query = db_table.build_sql_init(
                schema=os.environ.get("DATABASE_SCHEMA"),
                offset=offset,
                chunk_size=chunk_size,
            )
            # OFFSET funket ikke med ibm_db-pakka av en eller annen grunn. Må nok bruke noe annet, f.eks beregningsid range
            print(query)
            rows = fetch_data_chunk(db2_conn=conn, query=query)
            if not rows:
                print(f"There are no records to fetch for table {db_table.name}")
                break

            df_name = f"{db_table.name}_{offset // chunk_size}"
            df = pd.DataFrame(rows)

            memory_usage = df.memory_usage(deep=True).sum() / (1024**2)
            print(
                f"Fetched {len(df)} rows from {db_table.name}, offset {offset}, Memory Usage: from {df_name} : {memory_usage:.2f} MB"
            )
            # må øke offset med offset += chunk_size
            break

    except Exception as e:
        print(f"Skipping table {db_table.name}. Could not fetch data due to error: {e}")
