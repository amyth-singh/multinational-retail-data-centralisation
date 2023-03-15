#%%
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from sqlalchemy import create_engine
from yaml import Loader
import yaml
import pandas as pd
import psycopg2
import requests
import json

class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        yaml_file = open('db_creds.yaml', 'r')
        yaml_data = yaml.load(yaml_file, Loader=Loader)
        return yaml_data

    def read_my_db_creds(self):
        my_yaml_file = open('my_cred.yaml', 'r')
        my_yaml_data = yaml.load(my_yaml_file, Loader=Loader)
        return my_yaml_data

    def init_my_engine(self):
        my_cred = self.read_my_db_creds()
        return create_engine(f"postgresql+psycopg2://{my_cred['MY_USER']}:{my_cred['MY_PASSWORD']}@{my_cred['MY_HOST']}:{my_cred['MY_PORT']}/{my_cred['MY_DATABASE']}")

    def init_db_engine(self):
        cred = self.read_db_creds()
        return create_engine(f"postgresql+psycopg2://{cred['RDS_USER']}:{cred['RDS_PASSWORD']}@{cred['RDS_HOST']}:{cred['RDS_PORT']}/{cred['RDS_DATABASE']}")

    def upload_to_db(self, my_engine, clean_table):
        df = clean_table
        upload = df.to_sql(
            name='dim_users',
            con=my_engine,
            index=False,
            if_exists='replace'
        )
        return upload

    def upload_to_db_card(self, my_engine, clean_pdf_card_table):
        dfs = clean_pdf_card_table
        upload = dfs.to_sql(
            name='dim_card_details',
            con=my_engine,
            index=False,
            if_exists='replace'
        )
        return upload    

# Instantiation
databaseconnector = DatabaseConnector()
datacleaning = DataCleaning()
dataextractor = DataExtractor()

# Engines
engine = databaseconnector.init_db_engine()
my_engine = databaseconnector.init_my_engine()

# Credentials/Links
cred = databaseconnector.read_db_creds()
my_cred = databaseconnector.read_my_db_creds()
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

# API
header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
num_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

# Tables
raw_table = dataextractor.read_rds_table('legacy_users', engine)
clean_table = datacleaning.clean_user_data(raw_table)
raw_pdf_card_table = dataextractor.retrieve_pdf_data(pdf_link)
clean_pdf_card_table = datacleaning.clean_card_data(raw_pdf_card_table)
list_number_of_stores = dataextractor.list_number_of_stores(num_of_stores_endpoint, header)
retrieve_stores_data = dataextractor.retrieve_stores_data(retrieve_store_endpoint, header)

# Uploads to DB
upload = databaseconnector.upload_to_db(my_engine, clean_table)
upload_card = databaseconnector.upload_to_db_card(my_engine, clean_pdf_card_table)


# Workspace
retrieve_stores_data