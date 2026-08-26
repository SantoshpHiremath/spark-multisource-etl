"""
Real pytest suite exercising the actual PySpark ETL pipeline end to
end -- runs a real Spark session in local mode against the generated
raw source files, not mocked DataFrames.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pyspark.sql import SparkSession

from etl_pipeline import (
    extract_erp_orders, extract_crm_contacts, extract_product_catalog,
    clean_erp_orders, clean_crm_contacts, clean_product_catalog,
    build_revenue_mart, build_monthly_revenue_by_segment,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-multisource-etl")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def raw(spark):
    return {
        "erp": extract_erp_orders(spark),
        "crm": extract_crm_contacts(spark),
        "catalog": extract_product_catalog(spark),
    }


@pytest.fixture(scope="module")
def cleaned(raw):
    return {
        "orders": clean_erp_orders(raw["erp"]),
        "contacts": clean_crm_contacts(raw["crm"]),
        "catalog": clean_product_catalog(raw["catalog"]),
    }


@pytest.fixture(scope="module")
def mart(cleaned):
    return build_revenue_mart(cleaned["orders"], cleaned["contacts"], cleaned["catalog"])


# ---- Extraction ----

def test_erp_source_loads_all_raw_rows(raw):
    assert raw["erp"].count() == 415  # includes 15 injected duplicates and 10 missing-customer rows


def test_crm_source_loads_correctly_as_a_json_array_not_corrupt(raw):
    """Regression test for the real multiLine JSON bug caught while
    building this pipeline: without multiLine=True, Spark silently
    parsed the file as line-delimited JSON and produced a DataFrame
    with only a _corrupt_record column. This test pins the fix."""
    assert raw["crm"].count() == 10
    assert "crm_customer_id" in raw["crm"].columns
    assert "_corrupt_record" not in raw["crm"].columns


def test_catalog_source_loads_all_products(raw):
    assert raw["catalog"].count() == 5


# ---- Cleaning ----

def test_erp_cleaning_drops_rows_with_missing_customer_id(cleaned):
    assert cleaned["orders"].filter(cleaned["orders"].customer_id.isNull()).count() == 0
    assert cleaned["orders"].filter(cleaned["orders"].customer_id == "").count() == 0


def test_erp_cleaning_deduplicates_exact_duplicate_orders(cleaned):
    total = cleaned["orders"].count()
    distinct_order_ids = cleaned["orders"].select("order_id").distinct().count()
    assert total == distinct_order_ids  # no duplicate order_id should survive cleaning


def test_erp_amount_parsing_handles_german_locale_decimal_format(cleaned):
    """A real correctness check on the German-locale number parsing
    (comma decimal separator, period thousands separator) -- confirms
    an amount like '4.450,00' becomes the float 4450.00, not 4.45 or
    4450 with the wrong magnitude."""
    row = cleaned["orders"].filter(cleaned["orders"].order_id == "ORD-5001").collect()
    assert len(row) == 1
    assert row[0]["amount_eur"] == pytest.approx(4450.00, abs=0.01)


def test_erp_date_parsing_produces_valid_dates(cleaned):
    row = cleaned["orders"].limit(1).collect()[0]
    assert row["order_date"] is not None


def test_crm_cleaning_standardizes_customer_id_case_to_match_erp(cleaned):
    """The CRM source's lowercase 'c1001' must become uppercase
    'C1001' to match the ERP source's KundenNr convention -- otherwise
    the join in build_revenue_mart silently produces zero matches
    (Spark's default join is NOT case-insensitive)."""
    ids = [row["customer_id"] for row in cleaned["contacts"].select("customer_id").collect()]
    assert all(cid == cid.upper() for cid in ids)
    assert "C1001" in ids


def test_catalog_cleaning_strips_whitespace_and_normalizes_case(cleaned):
    codes = [row["product_code"] for row in cleaned["catalog"].select("product_code").collect()]
    assert all(code == code.strip().upper() for code in codes)
    assert "P02" in codes  # was " p02 " in the raw messy source


# ---- Join / mart ----

def test_revenue_mart_has_zero_unmatched_products(mart):
    unmatched = mart.filter(mart.product_name.isNull()).count()
    assert unmatched == 0


def test_revenue_mart_has_zero_unmatched_customers(mart):
    unmatched = mart.filter(mart.account_name.isNull()).count()
    assert unmatched == 0


def test_revenue_mart_row_count_matches_cleaned_orders(mart, cleaned):
    """A left join on cleaned, fully-matched keys should not change
    row count -- if it did, that would indicate a fan-out from a
    non-unique join key, a real bug class this test guards against."""
    assert mart.count() == cleaned["orders"].count()


def test_revenue_mart_total_matches_sum_of_cleaned_order_amounts(mart, cleaned):
    mart_total = mart.agg({"amount_eur": "sum"}).collect()[0][0]
    orders_total = cleaned["orders"].agg({"amount_eur": "sum"}).collect()[0][0]
    assert mart_total == pytest.approx(orders_total, abs=0.01)


# ---- Aggregation ----

def test_monthly_aggregate_total_matches_mart_total(mart):
    monthly = build_monthly_revenue_by_segment(mart)
    monthly_total = monthly.agg({"total_revenue_eur": "sum"}).collect()[0][0]
    mart_total = mart.agg({"amount_eur": "sum"}).collect()[0][0]
    assert monthly_total == pytest.approx(mart_total, abs=0.01)


def test_monthly_aggregate_order_counts_sum_to_mart_row_count(mart):
    monthly = build_monthly_revenue_by_segment(mart)
    monthly_order_count = monthly.agg({"order_count": "sum"}).collect()[0][0]
    assert monthly_order_count == mart.count()


def test_all_four_segments_appear_in_the_aggregate(mart):
    monthly = build_monthly_revenue_by_segment(mart)
    segments = {row["segment"] for row in monthly.select("segment").distinct().collect()}
    assert segments == {"Healthcare", "Education", "Pharma", "Association"}
