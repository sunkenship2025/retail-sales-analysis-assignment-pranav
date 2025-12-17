from data_loading import load_table_from_db
from processing import merge_sales_with_articles, add_total_and_date_columns
from analysis import (
    explore_dataframe, sales_per_branch, get_top_articles,
    calculate_monthly_revenue, calculate_category_revenue,
    save_metrics_to_db
)
import sqlite3
import logging
from utils import setup_logging

setup_logging(log_file="main.log")

# 1. Connect to DB
conn = sqlite3.connect("data/retail_sales.db")

# 2. Load data
df_branches = load_table_from_db("data/retail_sales.db", "branches")
df_articles = load_table_from_db("data/retail_sales.db", "articles")
df_sales = load_table_from_db("data/retail_sales.db", "sales")

# 3. Explore data (optional)
explore_dataframe(df_branches, "Branches")
explore_dataframe(df_articles, "Articles")
explore_dataframe(df_sales, "Sales")

# 4. Process data
sales_with_price = merge_sales_with_articles(df_sales, df_articles)
sales_with_price = add_total_and_date_columns(sales_with_price)

# 5. Analyze data
branch_sales = sales_per_branch(sales_with_price)
top_articles = get_top_articles(sales_with_price)
monthly_revenue = calculate_monthly_revenue(sales_with_price)
category_revenue = calculate_category_revenue(sales_with_price)

print("Branch Sales:\n", branch_sales)
logging.info(f"Branch Sales:\n{branch_sales}")

print("Top Articles:\n", top_articles)
logging.info(f"Top Articles:\n{top_articles}")

print("Monthly Revenue:\n", monthly_revenue)
logging.info(f"Monthly Revenue:\n{monthly_revenue}")

print("Category Revenue:\n", category_revenue)
logging.info(f"Category Revenue:\n{category_revenue}")

# 6. Save metrics to DB
save_metrics_to_db(conn, branch_sales, top_articles, monthly_revenue, category_revenue)

# 7. Close DB connection
conn.close()