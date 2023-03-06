from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import yaml
from yaml import Loader
import database_utils

# Extract data from multiple sources
class DataExtractor:
    def read_rds_data():
        data = database_utils.DatabaseConnector.read_db_creds()
        con = psycopg2.connect(user = data['RDS_USER'], password = data['RDS_PASSWORD'], host = data['RDS_HOST'], port = data['RDS_PORT'], database = data['RDS_DATABASE'])
        engine = database_utils.DatabaseConnector.init_db_engine()
        sql = 'SELECT * FROM legacy_users LIMIT 10;'
        df = pd.read_sql_query(sql,con)
        return df

a = DataExtractor
a.read_rds_data()