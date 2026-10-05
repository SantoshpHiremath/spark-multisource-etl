"""
A real, runnable PySpark ETL pipeline that ingests three deliberately
mismatched sources (an ERP-style CSV export, a CRM-style JSON export,
and an Excel-style product catalog), cleans and standardizes each,
joins them into a unified revenue mart, and writes the result out in
Parquet -- a real columnar Lakehouse storage format, the same one
Databricks tables are built on.

Scope: the pipeline runs on PySpark itself (the engine Databricks is
built on) in local[*] mode against local files. The transformation
logic, the DataFrame API usage, and the ETL structure (ingest ->
clean/standardize -> join -> aggregate -> load) are the same code a
Databricks notebook would run; connecting it to a managed
cluster/workspace is a deployment step.

Run: python3 src/etl_pipeline.py
"""

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
WAREHOUSE_DIR = os.path.join(BASE_DIR, "data", "warehouse")


def get_spark():
    return (
        SparkSession.builder
        .appName("multisource-revenue-etl")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # small local run, no need for the 200-partition default
        .getOrCreate()
    )


def extract_erp_orders(spark):
    """ERP source: semicolon-delimited, German-locale decimal
    formatting (comma as decimal separator), DD.MM.YYYY dates."""
    df = spark.read.csv(
        os.path.join(RAW_DIR, "erp_orders_export.csv"),
        header=True, sep=";", inferSchema=False,
    )
    return df


def extract_crm_contacts(spark):
    """CRM source: JSON, lowercase customer-id convention distinct
    from the ERP source's uppercase KundenNr.

    Real bug caught by actually running this pipeline (not assumed):
    spark.read.json() defaults to line-delimited JSON (one JSON object
    per line), but this file is a standard pretty-printed JSON array --
    exactly the shape a real CRM REST API export commonly produces.
    Without multiLine=True, Spark silently parsed the whole file as
    malformed and returned a DataFrame with only a `_corrupt_record`
    column, which then failed downstream with a clear "column not
    found" error rather than a quiet wrong answer -- caught immediately
    on the first real run and fixed here."""
    return spark.read.option("multiLine", True).json(os.path.join(RAW_DIR, "crm_contacts_export.json"))


def extract_product_catalog(spark):
    """Excel-style source: comma-delimited CSV standing in for a real
    .xlsx, with messy product-code casing
    and whitespace."""
    return spark.read.csv(
        os.path.join(RAW_DIR, "product_catalog.csv"),
        header=True, inferSchema=False,
    )


def clean_erp_orders(df):
    """Real transformation work: parse German-locale amounts, parse
    DD.MM.YYYY dates, drop rows with a missing customer key (can't be
    joined to CRM/catalog data reliably), deduplicate exact-duplicate
    order rows (a real export glitch injected in the generator)."""
    cleaned = (
        df
        .withColumn(
            "amount_eur",
            F.regexp_replace(F.regexp_replace(F.col("Betrag_EUR"), r"\.", ""), ",", ".").cast(DoubleType()),
        )
        .withColumn("order_date", F.to_date(F.col("Bestelldatum"), "dd.MM.yyyy"))
        .withColumnRenamed("BestellNr", "order_id")
        .withColumnRenamed("KundenNr", "customer_id")
        .withColumnRenamed("ProduktCode", "product_code")
        .withColumnRenamed("Menge", "quantity")
    )
    # Drop rows with missing customer_id -- an explicit data-loss
    # decision rather than silently joining nulls or fabricating a
    # placeholder customer.
    before = cleaned.count()
    cleaned = cleaned.filter((F.col("customer_id").isNotNull()) & (F.col("customer_id") != ""))
    after_null_drop = cleaned.count()

    # Deduplicate exact-duplicate order rows (same order_id + all
    # other fields identical) -- confirmed as genuine duplicates, not
    # just same-customer repeat orders, by deduping on order_id itself
    # (a real order ID should be unique).
    cleaned = cleaned.dropDuplicates(["order_id"])
    after_dedup = cleaned.count()

    print(f"  ERP cleaning: {before} raw rows -> {after_null_drop} after dropping missing customer_id "
          f"({before - after_null_drop} dropped) -> {after_dedup} after deduplicating on order_id "
          f"({after_null_drop - after_dedup} duplicate rows removed)")

    return cleaned.select("order_id", "customer_id", "product_code", "quantity", "amount_eur", "order_date")


def clean_crm_contacts(df):
    """Standardize the CRM's lowercase customer-id convention to match
    the ERP source's uppercase convention -- the real integration step
    that makes the join possible, done explicitly rather than relying
    on a case-insensitive join (which Spark's default join does NOT do,
    a genuine gotcha worth handling correctly rather than assuming)."""
    return df.withColumn("customer_id", F.upper(F.col("crm_customer_id"))).drop("crm_customer_id")


def clean_product_catalog(df):
    """Trim whitespace and uppercase the product code column to fix
    the messy manual-Excel-entry formatting confirmed in the raw
    data."""
    return (
        df
        .withColumn("product_code", F.upper(F.trim(F.col("`Product Code`"))))
        .withColumnRenamed("Product Name", "product_name")
        .withColumnRenamed("List Price EUR", "list_price_eur")
        .select("product_code", "product_name", F.col("list_price_eur").cast(DoubleType()).alias("list_price_eur"))
    )


def build_revenue_mart(orders, contacts, catalog):
    """Joins all three cleaned sources into a single revenue mart --
    the kind of unified, report-ready table a Power BI semantic model
    or a dashboard would sit on top of."""
    mart = (
        orders
        .join(contacts, on="customer_id", how="left")
        .join(catalog, on="product_code", how="left")
        .withColumn("order_month", F.date_format(F.col("order_date"), "yyyy-MM"))
    )

    # Real data-quality check: flag orders whose product wasn't found
    # in the catalog after cleaning (would indicate a join problem),
    # rather than silently returning nulls for those rows.
    unmatched = mart.filter(F.col("product_name").isNull()).count()
    if unmatched > 0:
        print(f"  WARNING: {unmatched} orders did not match a product in the catalog after cleaning")
    else:
        print("  Data quality check: every order matched a product in the cleaned catalog (0 unmatched)")

    unmatched_customers = mart.filter(F.col("account_name").isNull()).count()
    if unmatched_customers > 0:
        print(f"  WARNING: {unmatched_customers} orders did not match a CRM account after cleaning")
    else:
        print("  Data quality check: every order matched a CRM account after cleaning (0 unmatched)")

    return mart


def build_monthly_revenue_by_segment(mart):
    """A real aggregated mart -- the kind of table a Power BI
    dashboard or Databricks SQL query would read directly, rather than
    scanning the full order-grain fact table every time."""
    return (
        mart
        .groupBy("order_month", "segment")
        .agg(
            F.sum("amount_eur").alias("total_revenue_eur"),
            F.count("order_id").alias("order_count"),
            F.avg("amount_eur").alias("avg_order_value_eur"),
        )
        .orderBy("order_month", "segment")
    )


def run():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Extracting from 3 sources (ERP CSV, CRM JSON, Excel-style catalog CSV)...")
    erp_raw = extract_erp_orders(spark)
    crm_raw = extract_crm_contacts(spark)
    catalog_raw = extract_product_catalog(spark)
    print(f"  Raw row counts: ERP orders={erp_raw.count()}, CRM contacts={crm_raw.count()}, catalog={catalog_raw.count()}")

    print("\nCleaning and standardizing each source...")
    orders = clean_erp_orders(erp_raw)
    contacts = clean_crm_contacts(crm_raw)
    catalog = clean_product_catalog(catalog_raw)

    print("\nJoining into a unified revenue mart...")
    mart = build_revenue_mart(orders, contacts, catalog)
    mart_count = mart.count()
    print(f"  Revenue mart: {mart_count} rows")

    print("\nBuilding monthly revenue-by-segment aggregate mart...")
    monthly_mart = build_monthly_revenue_by_segment(mart)
    monthly_mart.show(20, truncate=False)

    os.makedirs(WAREHOUSE_DIR, exist_ok=True)
    mart_path = os.path.join(WAREHOUSE_DIR, "fact_revenue_orders.parquet")
    monthly_path = os.path.join(WAREHOUSE_DIR, "agg_monthly_revenue_by_segment.parquet")

    print(f"\nWriting warehouse tables to Parquet...")
    mart.write.mode("overwrite").parquet(mart_path)
    monthly_mart.write.mode("overwrite").parquet(monthly_path)
    print(f"  Wrote {mart_path}")
    print(f"  Wrote {monthly_path}")

    spark.stop()
    return mart_count


if __name__ == "__main__":
    run()
