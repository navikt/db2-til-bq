
import os
import pandas as pd
import ibm_db
import ibm_db_sa
from sqlalchemy import create_engine


def read_from_db2(table_name):

    database_username = os.environ.get("DATABASE_USERNAME")
    database_password = os.environ.get("DATABASE_PASSWORD")
    database_host = os.environ.get("DATABASE_HOST")
    database_port = os.environ.get("DATABASE_PORT")
    database_name = os.environ.get("DATABASE_NAME")
    schema = os.environ.get("DATABASE_SCHEMA")


    # Establish the connection
    db2_connection_string = f"db2+ibm_db://{database_username}:{database_password}@{database_host}:{database_port}/{database_name}"

    engine = create_engine(db2_connection_string)
    connection = engine.connect() 

    #run query
    sql = f"select * from {schema}.{table_name}"

    df = pd.read_sql(sql, connection)



def main():
    #lese inn data fra db2
    df = read_from_db2(table_name = 't_faggruppe')
    print(f"hentet {len(df)} rader")
    print("du er kul")

if __name__ == '__main__':
    main()
