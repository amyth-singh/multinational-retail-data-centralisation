from sqlalchemy import create_engine
from yaml import Loader
import yaml
import pandas as pd
import psycopg2

class DataExtractor:
    def __init__(self):
        pass

    def list_db_tables(self, cred):
        conn = psycopg2.connect(host=cred['RDS_HOST'], user=cred['RDS_USER'], password=cred['RDS_PASSWORD'], dbname=cred['RDS_DATABASE'], port=cred['RDS_PORT'])
        with conn:
            with conn.cursor() as cur:
                cur.execute('''SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';''')
                for table in cur.fetchall():
                    print(table)

    def read_rds_table(self, table_name, engine):
        df = pd.read_sql_table(table_name, engine)
        return df