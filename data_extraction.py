from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import yaml
from yaml import Loader
import database_utils

# Extract data from multiple sources
class DataExtractor:
    def read_rds_data():
        rds = database_utils.DatabaseConnector.list_db_table
        engine = database_utils.DatabaseConnector.init_db_engine
        table_1 = pd.read_sql_table('orders_table', engine)
        return table_1

data = DataExtractor
data.read_rds_data()