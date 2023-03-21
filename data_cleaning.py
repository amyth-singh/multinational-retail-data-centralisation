from data_extraction import DataExtractor
from sqlalchemy import create_engine
from yaml import Loader
import yaml
import pandas as pd
import psycopg2

class DataCleaning:
    def __init__(self):
        pass

    # Clean a Table
    def clean_user_data(self, raw_table):
        raw_table['address'] = raw_table['address'].str.replace('\n|/|\.|-|,', ' ', regex=True)
        raw_table['phone_number'] = raw_table['phone_number'].str.replace('\.|\(0\)|\(|\)|x', ' ', regex=True)
        raw_table.dropna()
        raw_table.set_index('index')
        return raw_table

    def clean_card_data(self, raw_pdf_card_table):
        raw_pdf_card_table.duplicated()
        raw_pdf_card_table.dropna()
        return raw_pdf_card_table

    def clean_store_data(self, retrieve_stores_data):
        df = retrieve_stores_data
        df.drop(columns='lat', inplace=True)
        df.set_index('index', inplace=True)

        # Drop unwanted Rows
        df.drop(axis=0, index=[0,63,172,217,231,333,381,405,414,447,437], inplace=True)

        # Format 'longitude' and 'latitude' columns
        df['longitude'] = df['longitude'].astype(float)
        df['latitude'] = df['latitude'].astype(float)

        # Clean 'continent' column
        df.loc[df['continent'] == 'eeEurope', 'continent'] = 'Europe'
        df.loc[df['continent'] == 'eeAmerica', 'continent'] = 'America'

        # Clearn 'opening_date' column
        df.loc[df['opening_date'] == '/', 'opening_date'] = '-'
        df['opening_date'] = pd.to_datetime(df['opening_date'])

        # Clean 'address' column
        df['address'] = df['address'].str.replace('\n|/|\.|-|,', ' ', regex=True)

        df.convert_dtypes()
        
        return df

    def convert_product_weights(self, raw_s3_products_data):
        df = raw_s3_products_data
        df.dropna()
        df.drop(index=[266,788,794,1660,1400,751,1133], inplace=True)
        df.reset_index(drop=True)
        
        # Columns get split on 'x'
        split_1 = df['weight'].str.split(pat='x', n=1, expand=True)

        # New Columns are assigned
        df['weight_g'] = split_1[1]
        df['no_items'] = split_1[0]

        # Strip characters (kg, g, .)
        df['weight_g'] = df['weight_g'].str.replace('[kg]|[.]', '', regex=True)
        df['no_items'] = df['no_items'].str.replace('[kg]|[.]|[ml]|[oz]', '', regex=True)

        # Replace Non-Type
        df['weight_g'] = df['weight_g'].fillna(value=0)

        # Change dtypes of new columns
        df['weight_g'] = df['weight_g'].astype(float)
        df['no_items'] = df['no_items'].astype(float)

        # Calculated column
        df['in_kg'] = df['weight_g'] * df['no_items'] / 1000
        df['in_kg'] = df['in_kg'].round(1)

        return df

    def clean_products_data(self, convert_product_weights):
        df = convert_product_weights
        return df

    def clean_orders_data(self, raw_orders_table):
        df = raw_orders_table
        df.drop(columns='1', inplace=True)
        df.drop(columns='level_0', inplace=True)
        df.drop(columns='first_name', inplace=True)
        df.drop(columns='last_name', inplace=True)
        df.set_index('index', inplace=True)
        return df