# %%
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import yaml
from yaml import Loader

class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        yaml_file = open('db_creds.yaml', 'r')
        yaml_data = yaml.load(yaml_file, Loader=Loader)
        return yaml_data

    def init_db_engine(self):
        cred = DatabaseConnector.read_db_creds(self)
        return create_engine(f"postgresql+psycopg2://{cred['RDS_USER']}:{cred['RDS_PASSWORD']}@{cred['RDS_HOST']}:{cred['RDS_PORT']}/{cred['RDS_DATABASE']}")

    def upload_to_db(self):
        pass