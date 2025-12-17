import pandas as pd
from analysis import sales_per_branch, get_top_articles, calculate_monthly_revenue, calculate_category_revenue,get_total_sales_per_branch,get_revenue_per_category,top5_selling_articles,monthly_sales_trend,sales_performance_by_city, save_metrics_to_db
import pytest
import sqlite3

@pytest.fixture
def mock_conn():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    # Create tables
    cursor.execute("""
        CREATE TABLE branches (
            branch_id INTEGER PRIMARY KEY,
            branch_name TEXT,
            city TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE articles (
            article_id INTEGER PRIMARY KEY,
            article_name TEXT,
            category TEXT,
            price REAL
        )
    """)
    cursor.execute("""
        CREATE TABLE sales_detail (
            transaction_id INTEGER PRIMARY KEY,
            branch_id INTEGER,
            article_id INTEGER,
            quantity INTEGER,
            sale_date TEXT,
            total_amount REAL,
            article_name TEXT,
            price REAL,
            category TEXT,
            month INTEGER,
            year INTEGER
        )
    """)
    # Insert minimal test data
    cursor.execute("INSERT INTO branches VALUES (1, 'Branch A', 'City X')")
    cursor.execute("INSERT INTO articles VALUES (1, 'Article A', 'Category X', 100.0)")
    cursor.execute("""INSERT INTO sales_detail VALUES 
        (1, 1, 1, 2, '2023-01-01', 200.0, 'Article A', 100.0, 'Category X', 1, 2023)
    """)
    conn.commit()
    yield conn
    conn.close()

def test_sales_per_branch(mock_conn):
    """
    Tests the calculation of total sales per branch.
    """
    sales_with_price = pd.DataFrame({
        "branch_id": [1, 1, 2],
        "total_amount": [500, 1000, 1500]
    })
    result = sales_per_branch(sales_with_price)
    assert result.loc[1] == 1500  # 500 + 1000
    assert result.loc[2] == 1500

def test_get_top_articles():
    """
    Tests the retrieval of top-selling articles.
    """
    sales_with_price = pd.DataFrame({
        "article_name": ["A", "B", "A"],
        "quantity": [5, 10, 15]
    })
    result = get_top_articles(sales_with_price)
    assert result.index[0] == "A"
    assert result.iloc[0] == 20  # 5 + 15
    assert result.index[1] == "B"
    assert result.iloc[1] == 10

def test_calculate_monthly_revenue():
    """
    Tests the calculation of monthly revenue.
    """
    sales_with_price = pd.DataFrame({
        "year": [2023, 2023, 2023],
        "month": [1, 1, 2],
        "total_amount": [500, 1000, 1500]
    })
    result = calculate_monthly_revenue(sales_with_price)
    assert result.loc[(2023, 1)] == 1500  # 500 + 1000
    assert result.loc[(2023, 2)] == 1500

def test_calculate_category_revenue():
    """
    Tests the calculation of total revenue per product category.
    """
    sales_with_price = pd.DataFrame({
        "category": ["Electronics", "Clothing", "Electronics"],
        "total_amount": [500, 1000, 1500]
    })
    result = calculate_category_revenue(sales_with_price)
    assert result.loc["Electronics"] == 2000  # 500 + 1500
    assert result.loc["Clothing"] == 1000

def test_get_total_sales_per_branch(mock_conn):
    """
    Tests the SQL query for total sales per branch.
    """
    result = get_total_sales_per_branch(mock_conn)
    assert not result.empty
    assert "branch_name" in result.columns
    assert "total_sales" in result.columns

def test_get_revenue_per_category(mock_conn):
    """
    Tests the SQL query for revenue per category.
    """
    result = get_revenue_per_category(mock_conn)
    assert not result.empty
    assert "category" in result.columns
    assert "total_revenue" in result.columns

def test_top5_selling_articles(mock_conn):
    """
    Tests the SQL query for top 5 selling articles, determined based on the total quantity sold.
    """
    result = top5_selling_articles(mock_conn)
    assert not result.empty
    assert "article_name" in result.columns
    assert "total_quantity" in result.columns
    assert len(result) <= 5

def test_monthly_sales_trend(mock_conn):
    """
    Tests the SQL query for monthly sales trend.
    """
    result = monthly_sales_trend(mock_conn)
    assert not result.empty
    assert "year" in result.columns
    assert "month" in result.columns
    assert "monthly_revenue" in result.columns
    assert "transaction_count" in result.columns

def test_sales_performance_by_city(mock_conn):
    """
    Tests the SQL query for sales performance by city.
    """
    result = sales_performance_by_city(mock_conn)
    assert not result.empty
    assert "city" in result.columns
    assert "total_revenue" in result.columns

import pandas as pd

def test_save_metrics_to_db(mock_conn):
    branch_sales = pd.DataFrame({"branch_id": [1], "total_sales": [200]})
    top_articles = pd.DataFrame({"article_name": ["Article A"], "total_quantity": [2]})
    monthly_revenue = pd.DataFrame({"year": [2023], "month": [1], "revenue": [200]})
    category_revenue = pd.DataFrame({"category": ["Category X"], "revenue": [200]})

    save_metrics_to_db(mock_conn, branch_sales, top_articles, monthly_revenue, category_revenue)
   

    