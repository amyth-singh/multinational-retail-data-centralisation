#%%
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import yaml
from yaml import Loader
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class Data:
    def __init__(self):
        pass
    
    def test(self):
        x = DatabaseConnector()
        y = x.init_db_engine()
        return y

a = Data()
a.test()

# %%
