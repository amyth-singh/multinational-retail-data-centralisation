
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import yaml
from yaml import Loader
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class DataCleaning:
    def __init__(self):
        pass

    # Clean a Table
    def clean_user_data(self, table_name):
        engine_2 = DataExtractor()
        clean_table = engine_2.read_rds_table(self, table_name)
        clean_table.set_index("index", inplace=True)
        clean_table['address'] = clean_table['address'].str.replace('\n|/|\.|-|,', ' ', regex=True)
        clean_table['phone_number'] = clean_table['phone_number'].str.replace('\.|\(0\)|\(|\)|x', ' ')
        clean_table.dropna()
        return clean_table
