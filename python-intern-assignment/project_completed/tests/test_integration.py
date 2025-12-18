import sqlite3
import tempfile
import os
import pandas as pd
from data_loading import load_table_from_db
from processing import merge_sales_with_articles, add_total_and_date_columns
from analysis import sales_per_branch

def test_integration_load_and_process():
    """
    Test loading and processing of sales and articles data.
    """
    #temp sqllite db file
    with tempfile.NamedTemporaryFile(suffix=".db",delete=False) as tmp:
        db_path = tmp.name
    try:
        #connect and create tables with sample data
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE sales (
                transaction_id INTEGER PRIMARY KEY,
                article_id INTEGER,
                quantity INTEGER,
                sale_date TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE articles (
                article_id INTEGER PRIMARY KEY,
                article_name TEXT,
                price REAL
            )
        """)
        cursor.execute("INSERT INTO articles VALUES (1, 'Article 1', 100.0)")
        cursor.execute("INSERT INTO sales VALUES (1, 1, 2, '2023-01-01')")
        conn.commit()
        conn.close()

        #load tables using function
        sales = load_table_from_db(db_path, "sales")
        articles = load_table_from_db(db_path, "articles")

        #process: merge and add calculated columns
        merged = merge_sales_with_articles(sales, articles)
        processed = add_total_and_date_columns(merged)

        #define expected results
        expected_data = pd.DataFrame({
            "transaction_id": [1],
            "article_id": [1],
            "quantity": [2],
            "sale_date": [pd.Timestamp("2023-01-01")],
            "article_name": ["Article 1"],
            "price": [100.0],
            "total_amount": [200.0],
            "year": [2023],
            "month": [1]
           
        })
        pd.testing.assert_frame_equal(
           processed[expected_data.columns].reset_index(drop=True),
           expected_data,
           check_dtype=False
       )
    finally:
        os.remove(db_path)

def test_integration_full_pipeline():
    """
    Test full integration from data loading to analysis.
    """
    # temp SQLite db file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    try:
        # connect and create tables with sample data
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE sales (
                transaction_id INTEGER PRIMARY KEY,
                article_id INTEGER,
                quantity INTEGER,
                sale_date TEXT,
                branch_id INTEGER
            )
        """)
        cursor.execute("""
            CREATE TABLE articles (
                article_id INTEGER PRIMARY KEY,
                article_name TEXT,
                price REAL
            )
        """)
        # Insert two sales for two branches
        cursor.execute("INSERT INTO articles VALUES (1, 'Article 1', 100.0)")
        cursor.execute("INSERT INTO sales VALUES (1, 1, 2, '2023-01-01', 1)")
        cursor.execute("INSERT INTO sales VALUES (2, 1, 3, '2023-01-02', 2)")
        conn.commit()
        conn.close()

        # load tables using function
        sales = load_table_from_db(db_path, "sales")
        articles = load_table_from_db(db_path, "articles")

        # process: merge and add calculated columns
        merged = merge_sales_with_articles(sales, articles)
        processed = add_total_and_date_columns(merged)

        # define expected processed results
        expected_data = pd.DataFrame({
            "transaction_id": [1, 2],
            "article_id": [1, 1],
            "quantity": [2, 3],
            "sale_date": [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")],
            "branch_id": [1, 2],
            "article_name": ["Article 1", "Article 1"],
            "price": [100.0, 100.0],
            "total_amount": [200.0, 300.0],
            "year": [2023, 2023],
            "month": [1, 1]
        })
        pd.testing.assert_frame_equal(
            processed[expected_data.columns].reset_index(drop=True),
            expected_data,
            check_dtype=False
        )

        # analysis: sales per branch
        branch_sales = sales_per_branch(processed)
        # expected: branch 1 has 200, branch 2 has 300
        assert branch_sales.loc[1] == 200.0
        assert branch_sales.loc[2] == 300.0

    finally:
        os.remove(db_path)