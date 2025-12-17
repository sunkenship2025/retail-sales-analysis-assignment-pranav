def merge_sales_with_articles(df_sales: pd.DataFrame, df_articles: pd.DataFrame) -> pd.DataFrame:
    """
    Merges sales DataFrame with articles DataFrame to include article details in sales data.
    """

    return df_sales.merge(df_articles, on='article_id', how='left')

def add_total_and_date_columns(sales_with_price: pd.DataFrame) -> pd.DataFrame:
    """
    Adds total amount and date-related columns to the sales DataFrame.
    """
    try:
        sales_with_price["total_amount"] = sales_with_price["quantity"] * sales_with_price["price"]
        sales_with_price["sale_date"] = pd.to_datetime(sales_with_price["sale_date"])
        sales_with_price["month"] = sales_with_price["sale_date"].dt.month
        sales_with_price["year"] = sales_with_price["sale_date"].dt.year
        return sales_with_price
    except Exception as e:
        print(f"Error occurred while adding total and date columns: {e}")
        return sales_with_price