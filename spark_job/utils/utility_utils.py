from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.sql.functions import (
    coalesce, current_date, col, to_timestamp, to_date, count, sum,
    row_number, countDistinct, current_timestamp, expr
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import os
import json
import sys
import logging
from pyspark.sql.utils import AnalysisException
from typing import Optional
from datetime import datetime


def log_message(message: str, level: str = "info"):
    """
    Logs a message to both console and a daily log file.

    The log file will be created as logs/YYYY-MM-DD.log.

    Args:
        message (str): The message to log.
        level (str): Logging level: 'info', 'warning', 'error', or 'debug'.
    """
    logger = logging.getLogger("ETLLogger")
    logger.setLevel(logging.INFO)

    # Setting the path for log file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
    today = datetime.now().strftime("%Y-%m-%d")
    log_file_path = os.path.join(parent_dir, "logs", f"{today}.log")
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    if not logger.handlers:
        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(ch)

        # File handler
        fh = logging.FileHandler(log_file_path)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)

    level = level.lower()
    if level == "info":
        logger.info(message)
    elif level == "warning":
        logger.warning(message)
    elif level == "error":
        logger.error(message)
    elif level == "debug":
        logger.debug(message)
    else:
        logger.info(message)


def init_spark(spark_config: dict)-> SparkSession:
    """
    Initialize and return a SparkSession.

    Args:
        spark_config (dict): Spark configuration dictionary.

    Returns:
        SparkSession: Initialized Spark session.
    """
    log_message("Initializing Spark session", "info")
    try:
        spark_conf = SparkConf().setAll(spark_config["spark_config"].items())
        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    except Exception as e:
        log_message(f"Initializing Spark failed with error: {e}", "error")
        raise
    return spark


def read_csv(path: str, spark: SparkSession) -> DataFrame:
    """
    Read a CSV file into a Spark DataFrame.

    Args:
        path (str): Path to the CSV file.
        spark (SparkSession): Spark session.

    Returns:
        DataFrame: Spark DataFrame containing CSV data.

    Raises:
        FileNotFoundError: If the path does not exist.
    """
    if not os.path.exists(path):
        log_message(f"CSV path does not exist: {path}", "error")
        raise FileNotFoundError(f"CSV path does not exist: {path}")

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )
        df.limit(1).count()  # Force evaluation to fail fast
        log_message(f"Successfully read CSV from path: {path}", "info")
        return df
    except Exception:
        log_message(f"Failed to read CSV from path: {path}", "error")
        raise


def read_json(file_path: str) -> dict:
    """
    Read a JSON file and return as a dictionary.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        dict: JSON contents as a Python dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def remove_nulls(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Remove rows with null values in the specified columns.

    Args:
        df (DataFrame): Input Spark DataFrame.
        columns (list[str]): List of columns to check for nulls.

    Returns:
        DataFrame: DataFrame with null rows removed.

    Raises:
        ValueError: If columns list is empty or contains missing columns.
        AnalysisException: If Spark fails due to invalid columns.
    """
    if not columns:
        log_message("Column list for remove_nulls is empty", "error")
        raise ValueError("Column list cannot be empty")

    try:
        missing_cols = [c for c in columns if c not in df.columns]
        if missing_cols:
            msg = f"Columns not found in DataFrame: {missing_cols}"
            log_message(msg, "error")
            raise ValueError(msg)

        cleaned_df = df.dropna(subset=columns)
        before_count = df.count()
        after_count = cleaned_df.count()
        log_message(
            f"Remove_nulls completed. Rows before: {before_count}, after: {after_count}, dropped: {before_count - after_count}",
            "info"
        )
        return cleaned_df
    except AnalysisException:
        log_message("Spark AnalysisException while removing nulls", "error")
        raise
    except Exception:
        log_message("Unexpected error while removing nulls", "error")
        raise


def prep_sales(df: DataFrame) -> DataFrame:
    """
    Prepare sales DataFrame by removing nulls and extracting ORDER_DATE.

    Args:
        df (DataFrame): Sales DataFrame.

    Returns:
        DataFrame: Prepared sales DataFrame.
    """
    df = remove_nulls(df, ["QUANTITY", "PRICE"])
    df = df.withColumn("ORDER_DATE", to_date(col("ORDER_TIMESTAMP")))
    return df


def cast_types(sales: DataFrame, 
               product: DataFrame, 
               exchange_rates: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Cast columns to appropriate data types for sales, product, and exchange rates.

    Args:
        sales (DataFrame): Sales DataFrame.
        product (DataFrame): Product DataFrame.
        exchange_rates (DataFrame): Exchange rates DataFrame.

    Returns:
        tuple: Casted sales, product, and exchange_rates DataFrames.
    """
    log_message("Casting datatypes before data enrichment", "info")

    sales = (
        sales
        .withColumn(
            "ORDER_TIMESTAMP",
            coalesce(
                to_timestamp(col("ORDER_TIMESTAMP"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("ORDER_TIMESTAMP"), "MM/dd/yyyy HH:mm"),
                to_timestamp(col("ORDER_TIMESTAMP"), "MM/dd/yyyy HH:mm:ss"),
                to_timestamp(col("ORDER_TIMESTAMP"), "yyyy/MM/dd HH:mm:ss")
            )
        )
        .withColumn("QUANTITY", col("QUANTITY").cast("int"))
        .withColumn("ORDER_ID", col("ORDER_ID").cast("int"))
        .withColumn("PRICE", col("PRICE").cast("decimal(12,2)"))
    )

    exchange_rates = exchange_rates.withColumn(
        "DATE",
        coalesce(
            to_date(col("DATE"), "yyyy-MM-dd HH:mm:ss"),
            to_date(col("DATE"), "MM/dd/yyyy"),
            to_date(col("DATE"), "MM/dd/yyyy HH:mm:ss"),
            to_date(col("DATE"), "MM/dd/yyyy HH:mm")
        )
    )

    return sales, product, exchange_rates


def enrich_sales(sales: DataFrame, product: DataFrame, exchange_rates: DataFrame) -> DataFrame:
    """
    Enrich sales DataFrame by joining with product and exchange rate information.

    Args:
        sales (DataFrame): Sales DataFrame.
        product (DataFrame): Product DataFrame.
        exchange_rates (DataFrame): Exchange rates DataFrame.

    Returns:
        DataFrame: Enriched sales DataFrame.
    """
    joined_df = sales.join(product, on="PRODUCT_ID", how="inner")
    s = joined_df.alias("s")
    e = exchange_rates.alias("e")

    joined_df = s.join(
        e,
        (col("s.ORDER_DATE") == col("e.DATE")) & (col("s.COUNTRY") == col("e.COUNTRY")),
        how="inner"
    ).select(
        col("s.ORDER_ID").alias("ORDER_ID"),
        col("s.CUSTOMER_ID").alias("CUSTOMER_ID"),
        col("s.PRODUCT_ID").alias("PRODUCT_ID"),
        col("s.ORDER_DATE").alias("ORDER_DATE"),
        col("s.ORDER_TIMESTAMP").alias("ORDER_TIMESTAMP"),
        col("s.COUNTRY").alias("COUNTRY"),
        col("s.CATEGORY").alias("CATEGORY"),
        col("s.QUANTITY").alias("QUANTITY"),
        col("s.PRICE").alias("PRICE"),
        col("e.EXCHANGE_RATE_TO_USD").alias("EXCHANGE_RATE_TO_USD")
    )
    # Adding Revenue_Local and Revenue_USD
    joined_df = joined_df.withColumn("REVENUE_LOCAL", col("QUANTITY") * col("PRICE")) \
                         .withColumn("REVENUE_USD", col("REVENUE_LOCAL") * col("EXCHANGE_RATE_TO_USD"))

    return joined_df


def daily_category_summary(df: DataFrame) -> DataFrame:
    """
    Aggregate sales data at day-category-country level.

    Args:
        df (DataFrame): Enriched sales DataFrame.

    Returns:
        DataFrame: Daily category summary.
    """
    return df.groupBy(col("ORDER_DATE"), col("COUNTRY"), col("CATEGORY")) \
             .agg(
                 countDistinct("ORDER_ID").alias("TOTAL_ORDERS"),
                 sum("QUANTITY").alias("TOTAL_QUANTITY"),
                 sum("REVENUE_LOCAL").alias("TOTAL_REVENUE_LOCAL"),
                 sum("REVENUE_USD").alias("TOTAL_REVENUE_USD")
             ) \
             .select(
                 "ORDER_DATE",
                 "COUNTRY",
                 "CATEGORY",
                 "TOTAL_ORDERS",
                 "TOTAL_QUANTITY",
                 "TOTAL_REVENUE_LOCAL",
                 "TOTAL_REVENUE_USD"
             )


def daily_customer_summary(df: DataFrame) -> DataFrame:
    """
    Aggregate sales data at day-customer-country level and identify top category.

    Args:
        df (DataFrame): Enriched sales DataFrame.

    Returns:
        DataFrame: Daily customer summary.
    """
    revenue_per_category = df.groupBy("ORDER_DATE", "CUSTOMER_ID", "CATEGORY", "COUNTRY") \
                             .agg(sum("REVENUE_USD").alias("CATEGORY_REVENUE_USD"))

    window_spec = Window.partitionBy(["CUSTOMER_ID", "ORDER_DATE", "COUNTRY"]) \
                        .orderBy(col("CATEGORY_REVENUE_USD").desc())

    df_top_category = revenue_per_category.withColumn("RANK_CATEGORY", row_number().over(window_spec)) \
                                          .filter(col("RANK_CATEGORY") == 1) \
                                          .select(col("CUSTOMER_ID").alias("CUSTOMER_ID"),
                                                   col("ORDER_DATE").alias("ORDER_DATE"),
                                                   col("COUNTRY").alias("COUNTRY"),
                                                   col("CATEGORY").alias("TOP_CATEGORY"))

    daily_customer_df = df.groupBy("ORDER_DATE", "CUSTOMER_ID", "COUNTRY") \
                          .agg(
                              countDistinct("ORDER_ID").alias("TOTAL_ORDERS"),
                              (sum(col("REVENUE_USD")) / count("ORDER_ID")).alias("AVG_ORDER_VALUE_USD")
                          )

    return daily_customer_df.join(df_top_category,
                                 on=["CUSTOMER_ID", "ORDER_DATE", "COUNTRY"],
                                 how="inner")
                                 


def writeResultsToFile(
    spark,
    base_directory: str,
    table_name: str,
    dataframe: DataFrame,
    inc_col: str,
    columns_to_merge_on: list[str],
    partitions: list[str] = None
):
    """
    Persist a DataFrame in Delta Lake format.

    Args:
        spark: Spark session.
        base_directory (str): Output directory.
        table_name (str): Logical table name.
        dataframe (DataFrame): DataFrame to write.
        inc_col (str): Column for incremental load.
        columns_to_merge_on (list[str]): Columns for merge condition.
        partitions (list[str], optional): Partition columns.
    """
    log_message(f"Writing results to {table_name}", "info")
    table_name = table_name.lower()
    delta_dir = os.path.join(base_directory, f"{table_name}")

    try:
        if os.path.exists(delta_dir):
            dataframe = dataframe.filter(col(inc_col) == current_date())
            merge_condition = None
            for column in columns_to_merge_on:
                if merge_condition is None:
                    merge_condition = (col(f"old.{column}") == col(f"new.{column}"))
                else:
                    merge_condition &= (col(f"old.{column}") == col(f"new.{column}"))

            delta_table = DeltaTable.forPath(spark, delta_dir)
            delta_table.alias("old").merge(dataframe.alias("new"), merge_condition) \
                       .whenMatchedUpdateAll() \
                       .whenNotMatchedInsertAll() \
                       .execute()
        else:
            dataframe.write.format("delta").mode("append").partitionBy(*partitions).save(delta_dir)

    except Exception as e:
        log_message(f"Writing to {table_name} failed with exception {e}", "error")


def validate_data_df(
    df: DataFrame,
    missing_cols: list[str],
    numeric_cols: list[str],
    date_col: str,
    rate_col: str
):
    """
    Validate business and data quality rules on a DataFrame.

    Args:
        df (DataFrame): Input DataFrame.
        missing_cols (list[str]): Columns that should not have nulls.
        numeric_cols (list[str]): Columns that must be positive.
        date_col (str): Timestamp column to check for recency/future.
        rate_col (str): Exchange rate column to validate.

    Raises:
        ValueError: If any validation rule fails.
    """
    errors = []

    # Check missing values
    for column in missing_cols:
        missing_count = df.filter(col(column).isNull()).count()
        if missing_count > 0:
            msg = f"Column '{column}' has {missing_count} missing values"
            log_message(msg, "warning")
            errors.append(msg)

    # Numeric positivity
    for column in numeric_cols:
        invalid_count = df.filter(col(column) <= 0).count()
        if invalid_count > 0:
            msg = f"Column '{column}' has {invalid_count} non-positive values"
            log_message(msg, "warning")
            errors.append(msg)

    # Timestamp within last 90 days
    ninety_days_ago = current_timestamp() - expr("INTERVAL 90 DAYS")
    invalid_date_count = df.filter((col(date_col).isNull()) | (col(date_col) < ninety_days_ago)).count()
    if invalid_date_count > 0:
        msg = f"Column '{date_col}' has {invalid_date_count} records outside last 90 days"
        log_message(msg, "warning")
        errors.append(msg)

    # Future timestamp
    future_date_count = df.filter(col(date_col) > current_timestamp()).count()
    if future_date_count > 0:
        msg = f"Column '{date_col}' has {future_date_count} future timestamp"
        log_message(msg, "warning")
        errors.append(msg)

    # Exchange rate validation
    invalid_rate_count = df.filter((col(rate_col).isNull()) | (col(rate_col) <= 0)).count()
    if invalid_rate_count > 0:
        msg = f"Column '{rate_col}' has {invalid_rate_count} invalid exchange rates"
        log_message(msg, "warning")
        errors.append(msg)

    if errors:
        raise ValueError("Data validation failed:\n" + "\n".join(errors))

    log_message("Data validation passed successfully", "info")
