def load_table_from_db(db_path:str,table_name:str):
    import sqlite3
    import pandas as pd
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    # Read the specified table into a DataFrame
    query = f"SELECT * FROM {table_name}" # we can use f-string to format the table name for example "articles" or "sales" in our case
    df = pd.read_sql_query(query, conn)
    # Close the database connection
    conn.close()
    return df