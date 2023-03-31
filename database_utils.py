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
import boto3
from io import StringIO
import tabula
import numpy as np

# AWS (CSV)
s3 = boto3.resource('s3')
s3_url = s3.Bucket('data-handling-public').Object('products.csv').get()
s3_products_data = s3_url['Body'].read()

# AWS (JSON)
s3_json = boto3.resource('s3')
s3_Json_url = s3.Bucket('data-handling-public').Object('date_details.json').get()
s3_date_details_data = s3_url['Body'].read()

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

# Upload to database functions

    def upload_to_db(self, my_engine, clean_user_data):
        df = clean_user_data
        upload = df.to_sql(
            name='dim_users',
            con=my_engine,
            index=False,
            if_exists='replace'
        )
        return upload

    def upload_to_db_card(self, my_engine, clean_pdf_card_data):
        dfs = clean_pdf_card_data
        upload = dfs.to_sql(
            name='dim_card_details',
            con=my_engine,
            index=False,
            if_exists='replace'
        )
        return upload

    def upload_to_db_stores_data(self, my_engine, clean_retrieve_stores_data):
        dfs = clean_retrieve_stores_data
        upload = dfs.to_sql(
            name='dim_store_details',
            con=my_engine,
            index=False,
            if_exists='replace'
        )
        return upload
        
    def upload_to_db_product_data(self, my_engine, clean_products_data):
        dfs = clean_products_data
        upload = dfs.to_sql(
            name='dim_products',
            con=my_engine,
            index=False,
            if_exists='replace'
        )
        return upload

    def upload_to_db_orders_data(self, my_engine, clean_orders_data):
        dfs = clean_orders_data
        upload = dfs.to_sql(
            name='orders_table',
            con=my_engine,
            index=False,
            if_exists='replace'
        )
        return upload

    def upload_to_db_date_times(self, my_engine, clean_date_times):
        dfs = clean_date_times
        upload = dfs.to_sql(
            name='dim_date_times',
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

# Links
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
json_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

# API
header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
num_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

# Tables
list_table_names = dataextractor.list_db_tables(cred)
list_number_of_stores = dataextractor.list_number_of_stores(num_of_stores_endpoint, header)

# Raw Tables
raw_user_data = dataextractor.read_rds_table('legacy_users', engine)
raw_orders_data = dataextractor.read_rds_table('orders_table', engine)
raw_pdf_card_data = dataextractor.extract_pdf_data(pdf_link)
raw_products_data = dataextractor.extract_products_data(s3_products_data)
raw_date_times = dataextractor.extract_date_times(json_link)
raw_stores_data = dataextractor.extract_stores_data(retrieve_store_endpoint, header)

# Clean Tables
clean_user_data = datacleaning.clean_user_data(raw_user_data)
clean_pdf_card_data = datacleaning.clean_card_data(raw_pdf_card_data)
clean_stores_data = datacleaning.clean_store_data(raw_stores_data)
clean_products_data = datacleaning.clean_products_data(raw_products_data)
clean_orders_data = datacleaning.clean_orders_data(raw_orders_data)
clean_date_times = datacleaning.clean_date_times(raw_date_times)

# Uploads to DB
upload_user_data = databaseconnector.upload_to_db(my_engine, clean_user_data)
upload_card_data = databaseconnector.upload_to_db_card(my_engine, clean_pdf_card_data)
upload_stores_data = databaseconnector.upload_to_db_stores_data(my_engine, clean_stores_data)
upload_products_data = databaseconnector.upload_to_db_product_data(my_engine, clean_products_data)
upload_orders_data = databaseconnector.upload_to_db_orders_data(my_engine, clean_orders_data)
upload_date_times_data = databaseconnector.upload_to_db_date_times(my_engine, clean_date_times)

# Workspace
#upload_products_data