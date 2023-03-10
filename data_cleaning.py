#%%
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