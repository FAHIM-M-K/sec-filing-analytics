# SEC Filing Analytics

A data engineering project that transforms SEC EDGAR filing data into a curated financial model for the current S&P 500 universe.

It retrieves SEC filing metadata and XBRL facts, standardizes selected financial metrics from 2010 onward, loads a star schema into Azure SQL, and exposes the results in Power BI Desktop.

For the business purpose, architecture, data model, and interview talking points, read [project_overview.md](project_overview.md).

## Data flow

![High-Level Architecture](images/highlevelview.png)

![High-Level Architecture](images/data_flow.png)

## What the pipeline does

- Retrieves SEC Company Submissions and Company Facts data for the companies listed in [`configs/sp500.csv`](configs/sp500.csv).
- Uses a declared SEC User-Agent, request throttling, retries, and exponential backoff.
- Records SHA-256 content hashes and ingestion metadata so unchanged source payloads can be skipped.
- Keeps 10-K, 10-Q, and amendment observations from 2010 onward.
- Maps selected standard US-GAAP tags to ten business metrics, retaining source tag, accession number, form, period, and source hash.
- Extracts selected 10-K narrative sections, including Item 1A risk factors.
- Builds Delta Lake Silver and Gold tables, then exports the Gold model to Azure SQL for Power BI.

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
