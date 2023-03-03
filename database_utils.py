# Libraries used
import yaml
from yaml import Loader

# Connect and upload data to the database
class DatabaseConnector:
    def read_db_creds():
        yaml_file = open('db_creds.yaml', 'r')
        data = yaml.load(yaml_file, Loader=Loader)
        return data

    def init_db_engine():
        data = DatabaseConnector.read_db_creds()
        database_type = 'postgresql'
        dbapi = 'psycopg2'
        host = data['RDS_HOST']
        password = data['RDS_PASSWORD']
        user = data['RDS_USER']
        database = data['RDS_DATABASE']
        port = data['RDS_PORT']
        engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}")
        connect = engine.connect()
        return connect

    def list_db_tebles():
        pass