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

# AWS
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

    def upload_to_db_date_times(self, my_engine, raw_s3_date_details_data):
        dfs = raw_s3_date_details_data
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

raw_table = dataextractor.read_rds_table('legacy_users', engine)
raw_orders_table = dataextractor.read_rds_table('orders_table', engine)
raw_pdf_card_table = dataextractor.retrieve_pdf_data(pdf_link)
raw_s3_products_data = dataextractor.extract_from_s3(s3_products_data)
raw_s3_date_details_data = dataextractor.extract_json_from_s3(json_link)

retrieve_stores_data = dataextractor.retrieve_stores_data(retrieve_store_endpoint, header)
convert_product_weights = datacleaning.convert_product_weights(raw_s3_products_data)

clean_user_table = datacleaning.clean_user_data(raw_table)
clean_pdf_card_table = datacleaning.clean_card_data(raw_pdf_card_table)
clean_retrieve_stores_data = datacleaning.clean_store_data(retrieve_stores_data)
clean_products_data = datacleaning.clean_products_data(convert_product_weights)
clean_orders_data = datacleaning.clean_orders_data(raw_orders_table)

# Uploads to DB
upload_clean_user_table = databaseconnector.upload_to_db(my_engine, clean_user_table)
upload_card = databaseconnector.upload_to_db_card(my_engine, clean_pdf_card_table)
upload_stores_data = databaseconnector.upload_to_db_stores_data(my_engine, clean_retrieve_stores_data)
upload_product_data = databaseconnector.upload_to_db_product_data(my_engine, clean_products_data)
upload_to_db_orders_data = databaseconnector.upload_to_db_orders_data(my_engine, clean_orders_data)
upload_to_db_date_times = databaseconnector.upload_to_db_date_times(my_engine, raw_s3_date_details_data)

# Workspace
upload_clean_user_table