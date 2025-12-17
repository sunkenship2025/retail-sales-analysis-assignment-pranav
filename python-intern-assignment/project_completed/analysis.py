import random
import sqlite3
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
import logging

import pandas as pd
from utils import setup_logging

setup_logging(log_file="analysis.log")

logging.basicConfig(level=logging.INFO)

def explore_dataframe(df: pd.DataFrame, name:str="DataFrame"):
    """
    Explores the given DataFrame and prints basic information, statistics, and missing values."""
    print(f"{name} Info:")
    print(df.info())
    print("\n")
    print(df.describe())
    print("\n")
    print("Missing values:")
    print(df.isnull().sum())
    print("\n")

    #buisness metrics functions




def sales_per_branch(sales_with_price: pd.DataFrame) -> pd.Series:
    try:
        """
        Calculates total sales per branch.
        """
        return sales_with_price.groupby("branch_id")["total_amount"].sum()
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return pd.Series(dtype=float)

def get_top_articles(sales_with_price: pd.DataFrame) -> pd.Series:
    """
    Gets the top-selling articles from the sales DataFrame.
    """
    try:
        return sales_with_price.groupby("article_name")["quantity"].sum().sort_values(ascending=False)
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return pd.Series(dtype=float)

def calculate_monthly_revenue(sales_with_price: pd.DataFrame) -> pd.Series:
    """
    Calculates monthly revenue from the sales DataFrame.
    """
    try:
        return sales_with_price.groupby(["year", "month"])["total_amount"].sum()
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return pd.Series(dtype=float)

def calculate_category_revenue(sales_with_price: pd.DataFrame) -> pd.Series:
    """
    Calculates total revenue per product category from the sales DataFrame.
    """
    try:
        return sales_with_price.groupby("category")["total_amount"].sum()
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return pd.Series(dtype=float)


#sql queries on sales data

def get_total_sales_per_branch(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Calculates total sales per branch.
    """
    try:
        query = """
        SELECT b.branch_name, SUM(s.total_amount) as total_sales
        FROM sales_detail s
        JOIN branches b ON s.branch_id = b.branch_id
        GROUP BY b.branch_name
        ORDER BY total_sales DESC
        """
        return pd.read_sql_query(query, conn)
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return pd.DataFrame()

def get_revenue_per_category(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Calculates total revenue per product category.
    """
    try:
        query = """
        SELECT a.category, SUM(s.total_amount) as total_revenue
        FROM sales_detail s
        JOIN articles a ON s.article_id = a.article_id
        GROUP BY a.category
        ORDER BY total_revenue DESC
        """
        return pd.read_sql_query(query, conn)
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return pd.DataFrame()

def top5_selling_articles(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Gets the top 5 selling articles from the sales data.
    """
    try:
        query = """
        SELECT a.article_name, SUM(s.quantity) as total_quantity
        FROM sales_detail s
        JOIN articles a ON s.article_id = a.article_id
        GROUP BY a.article_name
        ORDER BY total_quantity DESC
        LIMIT 5
        """
        return pd.read_sql_query(query, conn)
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return pd.DataFrame()

def monthly_sales_trend(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Gets the monthly sales trend from the sales data.
    """
    try:
        query = """
        SELECT year, month, SUM(total_amount) as monthly_revenue, COUNT(*) as transaction_count
        FROM sales_detail
        GROUP BY year, month
        ORDER BY year, month
        """
        return pd.read_sql_query(query, conn)
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return pd.DataFrame()


def sales_performance_by_city(conn: sqlite3.Connection) -> pd.DataFrame:
   """
   Gets the sales performance by city from the sales data.
   """
   try:
       query = """
       SELECT b.city, COUNT(DISTINCT b.branch_id) as num_branches, 
              SUM(s.total_amount) as total_revenue,
              AVG(s.total_amount) as avg_transaction_value
       FROM sales_detail s
       JOIN branches b ON s.branch_id = b.branch_id
       GROUP BY b.city
       ORDER BY total_revenue DESC
       """
       return pd.read_sql_query(query, conn)
   except Exception as e:
       logging.error(f"Error occurred: {e}")
       return pd.DataFrame()

def save_metrics_to_db(
    conn: sqlite3.Connection,
    sales_by_branch: pd.DataFrame,
    top_articles: pd.DataFrame,
    monthly_revenue: pd.DataFrame,
    category_revenue: pd.DataFrame
):
    """
    Saves the calculated business metrics to the database.
    """
    try:
        sales_by_branch = sales_by_branch.reset_index()
        sales_by_branch.columns = ["branch_id", "total_sales"]
        sales_by_branch.to_sql(
            "metrics_sales_by_branch", conn, if_exists="replace", index=False
        )

        top_articles.columns = ["article_name", "total_quantity"]
        top_articles.to_sql("metrics_top_articles", conn, if_exists="replace", index=False)

        monthly_revenue.columns = ["year", "month", "revenue"]
        monthly_revenue.to_sql(
            "metrics_monthly_revenue", conn, if_exists="replace", index=False
        )

        category_revenue.columns = ["category", "revenue"]
        category_revenue.to_sql(
            "metrics_category_revenue", conn, if_exists="replace", index=False
        )

        conn.commit()
        logging.info("\nAll business metrics saved to database successfully!")
    except Exception as e:
        logging.error(f"Error occurred while saving metrics to DB: {e}")