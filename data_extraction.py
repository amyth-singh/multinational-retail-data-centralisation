from sqlalchemy import create_engine
from yaml import Loader
import yaml
import pandas as pd
import psycopg2
import tabula
import requests
import json
import re
from io import StringIO

class DataExtractor:
    def __init__(self):
        pass

    def list_db_tables(self, cred):
        conn = psycopg2.connect(host=cred['RDS_HOST'], user=cred['RDS_USER'], password=cred['RDS_PASSWORD'], dbname=cred['RDS_DATABASE'], port=cred['RDS_PORT'])
        with conn:
            with conn.cursor() as cur:
                cur.execute('''SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';''')
                for table in cur.fetchall():
                    print(table)

    def read_rds_table(self, table_name, engine):
        df = pd.read_sql_table(table_name, engine)
        return df

    def extract_pdf_data(self, pdf_link):
        list_of_dfs = tabula.convert_into(pdf_link, "output.csv", output_format="csv", pages='all')
        df = pd.read_csv('output.csv')
        df1 = pd.DataFrame(df)
        return df1

    def list_number_of_stores(self, num_of_stores_endpoint, header):
        request = requests.get(num_of_stores_endpoint, headers=header)
        list_of_stores = json.loads(request.text)
        return list_of_stores['number_stores']

    def extract_stores_data(self, retrieve_store_endpoint, header):
        empty_list = []
        for i in range(0, 451): #451
            request = requests.get(retrieve_store_endpoint+str(i), headers=header)
            req_1 = json.loads(request.text)
            empty_list.append(req_1)
            stores_data = pd.DataFrame(empty_list)
        return stores_data

    def extract_products_data(self, s3_products_data):
        s=str(s3_products_data, 'utf-8')
        data_1 = StringIO(s)
        s3_products_data_df  = pd.read_csv(data_1, index_col=0)
        return s3_products_data_df

    def extract_date_times(self, json_link):
        empty_list = []
        r = requests.get(json_link)
        json_data = json.loads(r.text)
        empty_list.append(json_data)
        df = pd.DataFrame(empty_list[0])
        return df