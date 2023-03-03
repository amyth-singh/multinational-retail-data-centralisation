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
        read_db = DatabaseConnector.read_db_creds()
        return create_engine(f"postgresql+psycopg2://{read_db['RDS_USER']}:{read_db['RDS_PASSWORD']}@{read_db['RDS_HOST']}:{read_db['RDS_PORT']}/{read_db['RDS_DATABASE']}")


    def list_db_tables():
        pass