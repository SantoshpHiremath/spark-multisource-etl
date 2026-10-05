# Multi-Source ETL Pipeline (PySpark)

A runnable PySpark ETL pipeline that ingests three deliberately mismatched sources (an ERP-style CSV export, a CRM-style JSON export, and an Excel-style product catalog), cleans and standardizes each, joins them into a unified revenue mart, and writes the result to Parquet.

## What it does

The pipeline works against the source types a typical BI/data-analytics team faces: ERP and CRM exports and Excel files. It covers ingest, clean/standardize, join, aggregate, and load in a single PySpark job.

### The pipeline (`src/etl_pipeline.py`)

**Extract**: three sources, three different format quirks: semicolon-delimited CSV with German-locale amounts and `DD.MM.YYYY` dates (ERP); a pretty-printed JSON array (CRM); comma-delimited CSV with messy manual-entry formatting (product catalog).

**Clean**:
- ERP: parses `4.450,00` to `4450.00` (German-locale decimal handling), parses `DD.MM.YYYY` dates, drops rows with a missing customer key (an explicit data-loss decision rather than a silent null-join or a fabricated placeholder), and deduplicates on `order_id`, since the raw export has injected duplicate rows.
- CRM: standardizes the lowercase `crm_customer_id` convention to uppercase to match the ERP source's `KundenNr` convention. Spark's default join is not case-insensitive, so this step is what makes the join produce real matches.
- Catalog: trims whitespace and normalizes case on the product code column.

**Join & aggregate**: builds a row-grain revenue mart (order x customer x product), with explicit data-quality checks confirming zero unmatched products and zero unmatched customers after cleaning (checked and printed, not assumed). On top of it, a monthly revenue-by-segment aggregate mart: the kind of pre-aggregated table a BI dashboard would query directly.

**Load**: writes both the row-grain mart and the aggregate mart to Parquet under `data/warehouse/`.

## Data

All three source datasets are synthetic, modeled on a plausible B2B subscription/publishing business, and deliberately built with real messiness: German-locale number formatting (comma decimal separator) in the ERP export, a mismatched customer-ID casing convention between ERP and CRM sources, injected duplicate order rows, missing customer IDs on ~3% of ERP rows, and inconsistent whitespace/casing in the manually maintained product catalog. This is closer to what a real multi-source ETL job handles than three already-clean tables would be.

The Excel source is represented as CSV rather than a real `.xlsx`.

## Scope

The pipeline runs on PySpark itself, the engine Databricks is built on, in local mode (`local[*]`) against local files. The DataFrame transformation logic, the ingest, clean, join, aggregate, load structure, and the PySpark API calls (`.withColumn`, `.groupBy`, `.join`, window-free aggregation, Parquet writes) are the same code a Databricks notebook would run; connecting it to a managed cluster or workspace is a deployment step. Output is plain Parquet, the columnar format Delta Lake itself is built on, with PySpark 3.5.3. The scale is small (415 raw rows), enough to exercise real cleaning and join logic.

## Results

- `python3 src/etl_pipeline.py`: full end-to-end run: 415 raw ERP rows, 405 after dropping 10 rows with missing customer IDs, 390 after removing 15 duplicate order rows, 0 unmatched products, 0 unmatched customers, with the monthly revenue-by-segment output shown and written to Parquet.

## Tests

`python3 -m pytest tests/ -v`: 16 tests, all passing, run twice for stability. They cover raw row counts per source, the JSON multiLine regression, German-locale amount parsing (spot-checked against a specific known order), case-insensitive-join handling, zero-unmatched-join data-quality checks, and cross-checks that the aggregate mart's totals reconcile exactly against the row-grain mart's totals.

## Project structure

```
src/generate_raw_sources.py   generates the 3 synthetic raw sources
src/etl_pipeline.py           the ETL pipeline
data/raw/                     raw source files
data/warehouse/               Parquet output
tests/test_etl_pipeline.py    pytest suite
```

## Running it

```
python3 src/generate_raw_sources.py    # generates the 3 synthetic raw sources
python3 src/etl_pipeline.py             # runs the full ETL pipeline
python3 -m pytest tests/ -v             # 16 tests
```

## Notes

While building the pipeline, the first full run failed because `clean_crm_contacts()` referenced a column (`crm_customer_id`) that did not exist: `spark.read.json()` had parsed the whole CRM export into a single `_corrupt_record` column. Spark's JSON reader defaults to line-delimited JSON, but the CRM export is a standard pretty-printed JSON array, the shape a CRM REST API export commonly produces. The fix was one line (`spark.read.option("multiLine", True).json(...)`), found by running the pipeline and reading the actual error. A regression test (`test_crm_source_loads_correctly_as_a_json_array_not_corrupt`) pins it.

## Possible extensions

- Write Delta Lake tables instead of plain Parquet.
- Run the same notebook logic on a managed Databricks workspace.
- Add a live API source alongside the file-based ones.
