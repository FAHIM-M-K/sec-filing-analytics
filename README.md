# SEC Filing Analytics

A data engineering project that transforms SEC EDGAR filing data into a curated financial model for the current S&P 500 universe.

It retrieves SEC filing metadata and XBRL facts, standardizes selected financial metrics from 2010 onward, loads a star schema into Azure SQL, and exposes the results in Power BI Desktop.

For the business purpose, architecture, data model, and interview talking points, read [project_overview.md](project_overview.md).

## Data flow

![High-Level Architecture](images/highlevelview.png)

![High-Level Architecture](images/data_flow.png)

## What the pipeline does

- Retrieves SEC Company Submissions and Company Facts data for the companies listed in [`configs/sp500.csv`](configs/sp500.csv).
- Downloads older SEC submission-history chunks referenced by the current submissions index; filing/event parsers now fail clearly if a referenced chunk is absent or corrupt.
- Uses a declared SEC User-Agent, request throttling, retries, and exponential backoff.
- Records SHA-256 content hashes and ingestion metadata so unchanged source payloads can be skipped.
- Keeps 10-K, 10-Q, and amendment observations from 2010 onward.
- Maps selected standard US-GAAP tags to ten business metrics. Silver retains the source tag, accession, form, period start/end, and source hash for traceability.
- Extracts selected 10-K narrative sections, including Item 1A risk factors.
- Extracts high-impact Form 8-K catalyst item metadata into Silver and Gold corporate-event tables. The current extractor does not fetch filing HTML, so event headline and body fields are not populated yet.
- Preserves XBRL period start and end dates so standalone-quarter observations can be separated from cumulative year-to-date facts.
- Uses four latest discrete quarter-length revenue observations for LTM when they form a complete one-year window; if not, Power BI falls back to the latest annual revenue period. Both paths exclude periods before 2010.
- Builds Delta Lake Silver and Gold tables, then exports the Gold model to Azure SQL for Power BI.

After deploying a version that changes the reporting-period grain, run `silver/transform_observations` to rebuild the period-preserving Silver snapshot, then `gold/build_dimensions` and `gold/build_facts`. Before exporting, run this idempotent statement in the Azure SQL Query Editor to add the new period start column:

```sql
IF COL_LENGTH('dbo.dim_reporting_period', 'period_start') IS NULL
    ALTER TABLE dbo.dim_reporting_period ADD period_start VARCHAR(10) NULL;
```

Then run `serving/export_to_azure_sql`. Its default selection is `dim_reporting_period,fact_financial_metric`, so unrelated serving tables remain untouched. The notebook validates both SQL table schemas before writing and uses the SQL Server connector's overwrite-with-truncate mode to preserve the existing table definitions and indexes. Refresh the Power BI model after export completes.

## Repository layout

```text
configs/     Project configuration and the S&P 500 company list
notebooks/   Databricks ingestion, transformation, profiling, and export notebooks
src/         Reusable Python modules for each layer
tests/       Fast unit tests for non-Spark logic
docs/        Project documentation and architecture guides
```

## Local setup

```bash
git clone https://github.com/FAHIM-M-K/sec-filing-analytics.git
cd sec-filing-analytics
python -m venv .venv
.venv\Scripts\activate
python -m pip install -e ".[dev]"
pytest tests/unit
ruff check src tests
```

The ETL notebooks run in Databricks and require the configured secret scope for Azure SQL export. They are not intended to run as local Spark jobs.

## Scope

The project focuses on reported SEC fundamentals and filing disclosures. It does not provide market prices, returns, valuation multiples, custom XBRL facts, or complete segment-level analysis.

The S&P 500 source list is manually maintained. The pipeline preserves previously ingested data when a company leaves the list and begins historical ingestion when a new company is added.

## Stack

Python · PySpark · Delta Lake · Databricks Workflows · Azure SQL Database · Power BI Desktop · Pytest · Ruff
