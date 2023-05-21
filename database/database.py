from dotenv import load_dotenv
import os

load_dotenv()
database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')

def get_db_engine(database=database):
    from sqlalchemy import create_engine
    engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{database}')
    return engine