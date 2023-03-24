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
        object_columns = raw_table.select_dtypes(['object']).columns
        raw_table[object_columns] = raw_table[object_columns].replace('^[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]$', 'NaN', regex=True)
        df2 = raw_table.drop(raw_table[raw_table['date_of_birth'].str.contains("NaN|NULL")].index)
        df2 = raw_table.drop(raw_table[raw_table['phone_number'].str.contains("NaN|NULL")].index)
        df2['date_of_birth'] = pd.to_datetime(df2['date_of_birth'])
        df2['join_date'] = pd.to_datetime(df2['join_date'])
        df2.reset_index(drop=True)
        return df2

    def clean_card_data(self, raw_pdf_card_table):
        df = raw_pdf_card_table
        df.duplicated()
        df.dropna()
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'])
        return raw_pdf_card_table

    def clean_store_data(self, retrieve_stores_data):
        df = retrieve_stores_data
        df.drop(columns='lat', inplace=True)
        df['staff_numbers'] = df['staff_numbers'].replace(to_replace = '[a-zA-Z]', value = '', regex = True)
        df.loc[df['continent'] == 'eeEurope', 'continent'] = 'Europe'
        df.loc[df['continent'] == 'eeAmerica', 'continent'] = 'America'
        df = df.drop(df[df['address'].str.contains("NaN|NULL|N/A")].index)
        df['address'] = df['address'].str.replace('\n|/|\.', ' ', regex=True)
        o_c = df.select_dtypes(['object']).columns
        df[o_c] = df[o_c].replace('^[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]$', 'NaN', regex=True)
        df = df.drop(df[df['address'].str.contains("NaN|NULL")].index)
        df['opening_date'] = pd.to_datetime(df['opening_date'])
        df['longitude'] = df['longitude'].astype(float)
        df['latitude'] = df['latitude'].astype(float)
        df.reset_index(drop=True)
        return df

    def convert_product_weights(self, raw_s3_products_data):
        df = raw_s3_products_data

        # Dataframe cleaning
        o_c = df.select_dtypes(['object']).columns
        df[o_c] = df[o_c].replace('^[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]$', 'NaN', regex=True)
        df.drop(index=[266,751,788,794,1133,1400,1660,1705,1841], inplace=True)

        # Create columns
        df['grams'] = df['weight'].str.extract(r'(\d+g)') # Grams
        df['kilos'] = df['weight'].str.extract(r'(.+kg)') # Kilos
        df['mililitre'] = df['weight'].str.extract(r'(.+ml)') # Mililitre
        df['item_q'] = df['weight'].str.extract(r'(.+x)') # Item Quantity

        # Column cleaning
        df['grams'] = df['grams'].str.strip('gk')
        df['grams'].fillna(0, inplace=True)
        df['grams'] = df['grams'].astype(float)
        
        df['kilos'] = df['kilos'].str.strip('kg')
        df['kilos'].fillna(0, inplace=True)
        df['kilos'] = df['kilos'].astype(float)

        df['mililitre'] = df['mililitre'].str.strip('ml')
        df['mililitre'].fillna(0, inplace=True)
        df['mililitre'] = df['mililitre'].astype(int)

        df['item_q'] = df['item_q'].str.strip(' x')
        df['item_q'].fillna(0, inplace=True)
        df['item_q'] = df['item_q'].astype(int)

        df['product_price'] = df['product_price'].str.strip('£').astype(float)

        # Calculations
        df['grams-in-kg'] = df['grams'] / 1000
        df['ml-in-kg'] = df['mililitre'] / 1000
        df['x-grams'] = df['item_q'] * df['grams'] / 1000

        # Final columns
        df['in-kgs'] = df['grams-in-kg'] + df['ml-in-kg'] + df['x-grams'] + df['kilos'].astype(float)
        df['in-kgs'] = df['in-kgs'].round(1)
        df1 = df[['product_name', 'product_price', 'category', 'EAN', 'date_added', 'uuid', 'removed', 'product_code', 'weight', 'in-kgs']]
        df1 = df1.reset_index(drop=True)
    
        return df1

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

    def dim_date_times(self, raw_s3_date_details_data):
        df = raw_s3_date_details_data
        o_c = df.select_dtypes(['object']).columns
        df[o_c] = df[o_c].replace('^[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]$', 'NaN', regex=True)
        df = df.drop(df[df['timestamp'].str.contains("NaN|NULL")].index)
        return df