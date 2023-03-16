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
        df.drop(axis=0, index=0, inplace=True)
        df.drop(axis=0, index=63, inplace=True)
        df.drop(axis=0, index=172, inplace=True)
        df.drop(axis=0, index=217, inplace=True)
        df.drop(axis=0, index=231, inplace=True)
        df.drop(axis=0, index=333, inplace=True)
        df.drop(axis=0, index=381, inplace=True)
        df.drop(axis=0, index=405, inplace=True)
        df.drop(axis=0, index=414, inplace=True)
        df.drop(axis=0, index=447, inplace=True)
        df.drop(axis=0, index=437, inplace=True)

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