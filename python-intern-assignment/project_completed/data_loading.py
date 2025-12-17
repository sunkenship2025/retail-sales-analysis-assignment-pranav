import sqlite3
import pandas as pd
import logging
from utils import setup_logging

setup_logging(log_file="loading.log")

def load_table_from_db(db_path:str,table_name:str):
    try:
        logging.info(f"Loading table '{table_name}' from database.")
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        # Read the specified table into a DataFrame
        query = f"SELECT * FROM {table_name}" # we can use f-string to format the table name for example "articles" or "sales" in our case
        df = pd.read_sql_query(query, conn)
        # Close the database connection
        conn.close()
        logging.info(f"Loaded {len(df)} records from table '{table_name}'.")
        return df
    except Exception as e:
        logging.error(f"Error occurred while loading table '{table_name}': {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error