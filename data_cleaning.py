from data_extraction import DataExtractor
from sqlalchemy import create_engine
from yaml import Loader
import yaml
import pandas as pd
import psycopg2
import numpy as np

class DataCleaning:
    def __init__(self):
        pass

    # Clean a Table
    def clean_user_data(self, raw_table):
        df = raw_table
        df['address'] = df['address'].str.replace('\n|/|\.|-|,', '', regex=True)
        df['phone_number'] = df['phone_number'].str.replace('\.|\(0\)|\(|\)|x', '', regex=True)
        df.drop(index=[752, 1046, 2995, 3536, 5306, 6420, 8386, 9013, 10211, 10360, 11366, 12177, 13111, 14101, 14499], inplace=True)
        df = df.drop(df[df['user_uuid'].str.contains('NULL')].index)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
        df['join_date'] = pd.to_datetime(df['join_date'])
        df.reset_index(drop=True)
        df = df.drop(columns='index')
        return df

    def clean_card_data(self, extract_pdf_data):
        df = extract_pdf_data
        df.dropna(inplace=True)
        df = df.drop(df[df['card_number'].str.contains("card_number")].index)
        o_c = df.select_dtypes(['object']).columns
        df[o_c] = df[o_c].replace('^[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]$', 'drop_me', regex=True)
        df = df.drop(df[df['card_number'].str.contains('drop_me')].index)
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'])
        df['card_number'] = df['card_number'].str.strip('?')
        return df

    def clean_store_data(self, extract_stores_data):
        df = extract_stores_data
        df.drop(columns='lat', inplace=True)
        o_c = df.select_dtypes(['object']).columns
        df[o_c] = df[o_c].replace('^[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]$', 'drop_me', regex=True)
        df = df.drop(df[df['address'].str.contains('drop_me')].index)
        df = df.drop(df[df['address'].str.contains("NaN|NULL")].index)
        df.at[0,'latitude'] = '0.0000'
        df.at[0,'longitude'] = '0.0000'
        df.at[0,'locality'] = 'Web Store'
        df.at[0,'address'] = 'Web Store'
        df['address'] = df['address'].str.replace('\n|/|\.', ' ', regex=True)
        df['staff_numbers'] = df['staff_numbers'].replace(to_replace = '[a-zA-Z]', value = '', regex = True)
        df.loc[df['continent'] == 'eeEurope', 'continent'] = 'Europe'
        df.loc[df['continent'] == 'eeAmerica', 'continent'] = 'America'
        df['longitude'] = df['longitude'].astype(float)
        df['latitude'] = df['latitude'].astype(float)      
        df['opening_date'] = pd.to_datetime(df['opening_date'])
        df.reset_index(drop=True)
        return df

    def clean_products_data(self, raw_s3_products_data):
        df = raw_s3_products_data

        # Dataframe cleaning
        df.drop(index=[751,1133,1400,266,788,794,1660], inplace=True)

        # Create columns
        df['grams'] = df['weight'].str.extract(r'(\d+g)') # Grams
        df['kilos'] = df['weight'].str.extract(r'(.+kg)') # Kilos
        df['mililitre'] = df['weight'].str.extract(r'(.+ml)') # Mililitre
        df['item_q'] = df['weight'].str.extract(r'(.+x)') # Item Quantity
        df['oz'] = df['weight'].str.extract(r'(\d+oz)') # Oz

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

        df['oz'] = df['oz'].str.strip('oz')
        df['oz'].fillna(0, inplace=True)
        df['oz'] = df['oz'].astype(int)

        df['product_price'] = df['product_price'].str.strip('£').astype(float)

        # Calculations
        df['grams_in_kg'] = df['grams'] / 1000
        df['ml_in_kg'] = df['mililitre'] / 1000
        df['x_grams'] = df['item_q'] * df['grams'] / 1000
        df['oz_kg'] = df['oz'] * 0.0283495

        # Final columns
        df['in_kgs'] = df['grams_in_kg'] + df['ml_in_kg'] + df['x_grams'] + df['kilos'].astype(float) + df['oz_kg']
        df['in_kgs'] = df['in_kgs']
        df1 = df[['product_name', 'product_price', 'category', 'EAN', 'date_added', 'uuid', 'removed', 'product_code', 'weight', 'in_kgs']]
        df1['date_added'] = pd.to_datetime(df1['date_added'])
        df1['still_avaliable'] = np.where(df['removed'] == 'Still_avaliable', True, False)
        df1 = df1.reset_index(drop=True)
        return df1

    def clean_orders_data(self, raw_orders_table):
        df = raw_orders_table
        df.drop(columns=['first_name', 'last_name', 'level_0', '1', 'index'], inplace=True)
        df = df.drop_duplicates('user_uuid')
        df.reset_index(drop=True)
        return df

    def clean_date_times(self, raw_date_times):
        df = raw_date_times
        df = df.drop_duplicates('date_uuid')
        o_c = df.select_dtypes(['object']).columns
        df[o_c] = df[o_c].replace('^[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]$', 'NaN', regex=True)
        df = df.drop(df[df['date_uuid'].str.contains("NaN|NULL")].index)
        return df