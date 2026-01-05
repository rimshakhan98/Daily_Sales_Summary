import sys
import os

# Add project root to sys.path to import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import utility_utils as uu


def main():
    """
    Main ETL workflow:
    1. Initialize Spark session
    2. Load CSV data for sales, products, and exchange rates
    3. Clean, cast, and prepare data
    4. Enrich sales data with product and exchange rate info
    5. Validate enriched data
    6. Aggregate daily summaries by category and customer
    7. Persist results as Delta/Parquet with incremental load simulation
    """
    
    # -----------------------------
    # Define project root & configs
    # -----------------------------
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    spark_config_path = os.path.join(project_root, "config", "spark_config.json")
    paths_config_path = os.path.join(project_root, "config", "paths.json")
    
    # -----------------------------
    # Initialize Spark session
    # -----------------------------
    spark_config = uu.read_json(spark_config_path)
    spark = uu.init_spark(spark_config)

    # -----------------------------
    # Load CSV paths from config
    # -----------------------------
    paths = uu.read_json(paths_config_path)
    sales_path = paths.get("sales_path")
    products_path = paths.get("products_path")
    exchange_rates_path = paths.get("exchange_rates_path")

    # -----------------------------
    # Read CSV files
    # -----------------------------
    sales_df = uu.read_csv(sales_path, spark)
    products_df = uu.read_csv(products_path, spark)
    exchange_rates_df = uu.read_csv(exchange_rates_path, spark)

    # -----------------------------
    # Clean & prepare data
    # -----------------------------
    sales_df, products_df, exchange_rates_df = uu.cast_types(sales_df, products_df, exchange_rates_df)
    sales_df = uu.prep_sales(sales_df)

    # -----------------------------
    # Enrich sales data
    # -----------------------------
    joined_df = uu.enrich_sales(sales_df, products_df, exchange_rates_df)

    # -----------------------------
    # Validate enriched data
    # -----------------------------
    uu.validate_data_df(
        joined_df,
        missing_cols=["COUNTRY", "PRICE", "QUANTITY"],
        numeric_cols=["QUANTITY", "PRICE", "REVENUE_LOCAL", "REVENUE_USD"],
        date_col="ORDER_TIMESTAMP",
        rate_col="EXCHANGE_RATE_TO_USD"
    )

    # -----------------------------
    # Aggregations
    # -----------------------------
    category_summary = uu.daily_category_summary(joined_df)
    customer_summary = uu.daily_customer_summary(joined_df)

    # -----------------------------
    # Persist results (incremental load simulation)
    # -----------------------------
    output_path = os.path.join(project_root, "output")

    # Category summary
    uu.writeResultsToFile(
        spark,
        base_directory=output_path,
        table_name="category_summary",
        dataframe=category_summary,
        inc_col="ORDER_DATE",
        columns_to_merge_on=["COUNTRY", "ORDER_DATE", "CATEGORY"],
        partitions=["COUNTRY"]
    )

    # Customer summary
    uu.writeResultsToFile(
        spark,
        base_directory=output_path,
        table_name="customer_summary",
        dataframe=customer_summary,
        inc_col="ORDER_DATE",
        columns_to_merge_on=["COUNTRY", "ORDER_DATE", "CUSTOMER_ID"],
        partitions=["ORDER_DATE"]
    )


if __name__ == "__main__":
    main()
