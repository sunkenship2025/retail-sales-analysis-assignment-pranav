import pandas as pd
from processing import merge_sales_with_articles, add_total_and_date_columns


def test_merge_sales_with_articles():
    """
    Tests the merging of sales and articles DataFrames."""

    sales=pd.DataFrame({
        "article_id":[1,2],
        "quantity":[5,10]
        })
    articles = pd.DataFrame({
        "article_id": [1,2],
        "price":[100,200]
    })
    #call the function
    merged_df = merge_sales_with_articles(sales, articles)
    #check if the merge was successful
    assert "price" in merged_df.columns
    assert merged_df.loc[0, "price"] == 100
    assert merged_df.loc[1, "price"] == 200
    assert len(merged_df) == 2

def test_add_total_and_date_columns():
    """
    Tests the addition of total amount and date-related columns to the sales DataFrame.
    """
    sales_with_price = pd.DataFrame({
        "quantity": [5, 10],
        "price": [100, 200],
        "sale_date": ["2023-01-15", "2023-02-20"]
        })
        # Call the function
    processed_df = add_total_and_date_columns(sales_with_price)
        # Check if the new columns are added correctly
    assert "total_amount" in processed_df.columns
    assert processed_df.loc[0, "total_amount"] == 500  # 5 * 100
    assert processed_df.loc[1, "total_amount"] == 2000  #   10 * 200
    assert "month" in processed_df.columns
    assert "year" in processed_df.columns
    assert processed_df.loc[0, "month"] == 1
    assert processed_df.loc[0, "year"] == 2023 