import sqlite3
import pandas as pd
import tempfile
import os
from data_loading import load_table_from_db

def test_load_table_from_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                age INTEGER
            )
        """)
        cursor.execute("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        cursor.execute("INSERT INTO test_table (name, age) VALUES ('Bob', 25)")
        conn.commit()
        conn.close()

        df = load_table_from_db(db_path, "test_table")
        assert not df.empty
        assert "id" in df.columns
        assert "name" in df.columns
        assert "age" in df.columns
        assert df.shape[0] == 2
    finally:
        os.remove(db_path)