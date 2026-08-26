# Multi-Source ETL Pipeline (PySpark)

A real, runnable PySpark ETL pipeline that ingests three deliberately mismatched sources — an ERP-style CSV export, a CRM-style JSON export, and an Excel-style product catalog — cleans and standardizes each, joins them into a unified revenue mart, and writes the result to Parquet. Built to close a named gap: an ETL pipeline against exactly the source types a BI/data-analytics posting names ("ERP- und CRM-Systeme, Excel-Dateien oder APIs... mithilfe von ETL-Prozessen").

## Honest disclosure — read before citing anywhere

**This sandbox has no live Databricks workspace, cluster, DBFS, or Unity Catalog.** What genuinely runs here is PySpark itself — the real engine Databricks is built on — in local mode (`local[*]`), against local files. The DataFrame transformation logic, the ingest → clean/standardize → join → aggregate → load structure, and the actual PySpark API calls (`.withColumn`, `.groupBy`, `.join`, window-free aggregation, Parquet writes) are the same code a Databricks notebook would run; only the managed cluster/workspace layer is not something I have access to in this environment. Stated directly rather than implied as more than it is.

**All three source datasets are synthetic**, modeled on a plausible B2B subscription/publishing business (not real Thieme or any company's data, which I have no access to) — but deliberately built with real messiness: German-locale number formatting (comma decimal separator) in the ERP export, a mismatched customer-ID casing convention between ERP and CRM sources, injected duplicate order rows, missing customer IDs on ~3% of ERP rows, and inconsistent whitespace/casing in the manually-maintained product catalog. This is closer to what a real multi-source ETL job actually has to handle than three already-clean tables would be.

**The Excel source is represented as CSV**, not a real `.xlsx` — this sandbox has no Windows Excel to author one, the same disclosed constraint as `powerquery-sap-reporting`.

## A real bug found and fixed while building this

The first full pipeline run failed immediately: `clean_crm_contacts()` tried to reference a column (`crm_customer_id`) that didn't exist, because `spark.read.json()` had silently parsed the whole CRM export file into a single `_corrupt_record` column instead of real fields. The cause: Spark's JSON reader defaults to line-delimited JSON (one JSON object per line), but the CRM export file is a standard pretty-printed JSON array — exactly the shape a real CRM REST API export commonly produces. The fix was one line (`spark.read.option("multiLine", True).json(...)`), but finding it required actually running the pipeline and reading the real error, not assuming the reader call was correct because it looked standard. A regression test (`test_crm_source_loads_correctly_as_a_json_array_not_corrupt`) pins this so it can't silently reappear.

## Another real issue, caught and fixed before it could affect other work

While setting up this project, I initially installed `delta-spark` (to write genuine Delta Lake format instead of plain Parquet) — this pulled in PySpark 4.2.0 and upgraded Flask/Werkzeug as dependencies. That silently broke an already-verified, unrelated project in this portfolio (`dbt-airflow-orchestration`): its Airflow DAG-execution test started failing with a Flask import error. I caught this by re-running that project's test suite after the install (21 tests, 1 failure) rather than assuming an unrelated package install was safe, reverted to plain PySpark 3.5.3 and the original Flask/Werkzeug versions, and re-confirmed all 21 of that project's tests passed again before continuing here. This project therefore uses plain Parquet (a real, standard Lakehouse-format columnar file, and what Delta Lake itself is built on) rather than the Delta table format specifically — a smaller claim than "wrote a Delta table," stated honestly rather than glossed over.

## The pipeline (`src/etl_pipeline.py`)

**Extract**: three sources, three different real format quirks —semicolon-delimited CSV with German-locale amounts and `DD.MM.YYYY` dates (ERP); a pretty-printed JSON array (CRM); comma-delimited CSV with messy manual-entry formatting (product catalog).

**Clean**:
- ERP: parses `4.450,00` → `4450.00` (German-locale decimal handling), parses `DD.MM.YYYY` dates, drops rows with a missing customer key (a disclosed data-loss decision rather than a silent null-join or a fabricated placeholder), deduplicates on `order_id` — the raw export has genuine injected duplicate rows.
- CRM: standardizes the lowercase `crm_customer_id` convention to uppercase to match the ERP source's `KundenNr` convention — Spark's default join is not case-insensitive, so this step is what actually makes the join produce real matches rather than silently returning nothing.
- Catalog: trims whitespace and normalizes case on the product code column.

**Join & aggregate**: builds a row-grain revenue mart (order × customer × product), with explicit data-quality checks confirming zero unmatched products and zero unmatched customers after cleaning (not assumed — checked and printed). Builds a monthly revenue-by-segment aggregate mart on top of it, the kind of pre-aggregated table a BI dashboard would query directly.

**Load**: writes both the row-grain mart and the aggregate mart to Parquet under `data/warehouse/`.

## Verification performed (all real, run live against this exact code)

- `python3 -m pytest tests/ -v` — 16 tests, all passing, run twice for stability. Tests cover: raw row counts per source, the JSON multiLine regression, German-locale amount parsing correctness (spot-checked against a specific known order), case-insensitive-join handling, zero-unmatched-join data-quality checks, and cross-checks that the aggregate mart's totals reconcile exactly against the row-grain mart's totals (a real correctness property, not just "did it run").
- `python3 src/etl_pipeline.py` — full end-to-end run: 415 raw ERP rows → 405 after dropping 10 rows with missing customer IDs → 390 after removing 15 duplicate order rows, 0 unmatched products, 0 unmatched customers, monthly revenue-by-segment output shown and written to Parquet.

## Running it

```
python3 src/generate_raw_sources.py    # generates the 3 synthetic raw sources
python3 src/etl_pipeline.py             # runs the full ETL pipeline
python3 -m pytest tests/ -v             # 16 tests
```

## Scope and limitations, disclosed directly

- No live Databricks workspace/cluster — local PySpark only.
- Synthetic data, not a real company's ERP/CRM export.
- Plain Parquet output, not Delta Lake table format (see the dependency-conflict disclosure above).
- Small scale (415 raw rows) — enough to exercise real cleaning/join logic correctly, not a claim of big-data-scale processing.
