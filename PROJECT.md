# SEC Filing Analytics — Project Documentation

## Overview

A production-grade **ETL pipeline** that ingests SEC EDGAR filings for all 499 S&P 500 companies, transforms the data through three medallion layers (Bronze → Silver → Gold), and stores it in Databricks Delta Lake, ready for analytics and downstream serving.

**Data source:** SEC EDGAR public APIs (`data.sec.gov`)  
**Universe:** S&P 500 companies (499 tracked via `configs/sp500.csv`)  
**Pipeline orchestration:** Databricks Serverless Jobs  
**Storage:** Delta Lake on Databricks (`sec_analytics` Unity Catalog)

---

## Architecture — Medallion Layers

```
SEC EDGAR API
     │
     ▼
┌─────────────────────────────────────┐
│  BRONZE  (raw, as-is from source)  │
│  - submission_manifest              │
│  - company_facts_manifest           │
│  - gzipped payloads in Volume       │
└───────────────┬─────────────────────┘
                │
                ▼
┌─────────────────────────────────────┐
│  SILVER  (cleaned, standardised)   │
│  - filing                           │
│  - observation                      │
│  - filing_narrative                 │
└───────────────┬─────────────────────┘
                │
                ▼
┌─────────────────────────────────────┐
│  GOLD  (star schema, business-ready)│
│  Dimensions: dim_company,            │
│    dim_filing, dim_reporting_period, │
│    dim_metric                        │
│  Facts: fact_financial_metric,       │
│    fact_narrative                    │
└─────────────────────────────────────┘
```

---

## Pipeline DAG (Databricks Job)

```
ingest_submissions ──► transform_filings ──► extract_narratives ──┐
                                          └──► build_dimensions ──►  build_narratives
ingest_company_facts ──► transform_observations ──────────────────►  build_dimensions
                                                                       └──► build_facts
```

| Task | Layer | Notebook |
|---|---|---|
| `ingest_submissions` | Bronze | `notebooks/bronze/ingest_submissions` |
| `ingest_company_facts` | Bronze | `notebooks/bronze/ingest_company_facts` |
| `transform_filings` | Silver | `notebooks/silver/transform_filings` |
| `transform_observations` | Silver | `notebooks/silver/transform_observations` |
| `extract_narratives` | Silver | `notebooks/silver/extract_narratives` |
| `build_dimensions` | Gold | `notebooks/gold/build_dimensions` |
| `build_facts` | Gold | `notebooks/gold/build_facts` |
| `build_narratives` | Gold | `notebooks/gold/build_narratives` |

Scheduled: **Every Sunday at 4:45 AM GST** (Asia/Dubai timezone)

---

## Bronze Layer

**Purpose:** Land raw SEC EDGAR API responses as immutable gzipped files. Maintain a manifest table tracking content hashes so only changed payloads trigger re-processing.

**Tables:**
- `sec_analytics.bronze.submission_manifest` — one row per company, tracks submission JSON hash and file path
- `sec_analytics.bronze.company_facts_manifest` — one row per company, tracks company-facts JSON hash and file path

**Key design decisions:**
- Payloads stored as `.json.gz` in a Databricks Volume (`/Volumes/sec_analytics/bronze/raw/`)
- `content_hash` (SHA-256) on each payload — if unchanged, skip processing downstream
- SEC EDGAR rate limit: 10 req/sec with exponential backoff (5 retries)

**Source files:**
- `src/sec_filing_analytics/bronze/submissions.py`
- `src/sec_filing_analytics/bronze/company_facts.py`
- `src/sec_filing_analytics/sec_client/client.py`

---

## Silver Layer

**Purpose:** Parse and normalise Bronze payloads into typed, deduplicated Delta tables. All writes are idempotent MERGEs.

### `silver.filing`

One row per unique filing (accession number). Sourced from submission JSON.

| Column | Type | Notes |
|---|---|---|
| `cik` | STRING | 10-digit zero-padded |
| `accession_number` | STRING | dashed format `0000320193-24-000123` |
| `form_type` | STRING | `10-K`, `10-Q`, `10-K/A`, `10-Q/A` |
| `filing_date` | STRING | `YYYY-MM-DD` |
| `report_date` | STRING | `YYYY-MM-DD` |
| `fiscal_year` | INT | |
| `fiscal_period` | STRING | `FY`, `Q1`–`Q4` |
| `company_name` | STRING | |
| `primary_document` | STRING | main HTM/HTML file name |
| `source_hash` | STRING | lineage back to Bronze |

**Current row count:** 18,227

### `silver.observation`

One row per unique financial data point. Sourced from company-facts XBRL JSON.

| Column | Type | Notes |
|---|---|---|
| `cik` | STRING | 10-digit zero-padded |
| `metric` | STRING | canonical label (e.g. `"Revenue"`) |
| `tag` | STRING | raw XBRL tag |
| `period_type` | STRING | `duration` or `instant` |
| `period_start` | STRING | null for balance sheet (instant) metrics |
| `period_end` | STRING | |
| `fiscal_year` / `fiscal_period` | INT/STRING | |
| `unit` | STRING | `USD` or `USD/shares` |
| `value` | DOUBLE | |

**10 canonical metrics:**

| Metric | Unit | Statement |
|---|---|---|
| Revenue | USD | Income |
| Net Income | USD | Income |
| Operating Income | USD | Income |
| Operating Cash Flow | USD | Cash Flow |
| EPS Diluted | USD/shares | Per Share |
| EPS Basic | USD/shares | Per Share |
| Total Assets | USD | Balance Sheet |
| Total Liabilities | USD | Balance Sheet |
| Cash | USD | Balance Sheet |
| Long-term Debt | USD | Balance Sheet |

**Deduplication:** Two-pass window function — first picks the latest-filed version of a tag for the same period, then resolves canonical metric conflicts by XBRL tag priority.

**Current row count:** 399,482

### `silver.filing_narrative`

One row per extracted text section per filing.

| Column | Type | Notes |
|---|---|---|
| `cik` | STRING | |
| `accession_number` | STRING | |
| `form_type` | STRING | |
| `filing_date` | STRING | |
| `item_name` | STRING | e.g. `"Item 1A"`, `"Item 7"` |
| `text_content` | STRING | cleaned, HTML-stripped |
| `char_count` | INT | |
| `source_hash` | STRING | MD5 of content |

**Extraction logic:** Regex-based section boundary detection from live SEC EDGAR HTML. Skips TOC entries (page-number-only lines). Picks the longest candidate match.

---

## Gold Layer — Star Schema

**Purpose:** Business-ready dimensional model. All surrogate keys are deterministic SHA-256 hash keys (reproducible across runs). All writes are idempotent MERGEs on natural keys.

### Surrogate Key Formula

```python
conv(substring(sha2(concat_ws("|", *natural_key_cols), 256), 1, 15), 16, 10)
```

Takes the first 15 hex chars of a SHA-256 hash and converts to a BIGINT — deterministic, collision-resistant for this dataset size.

### `gold.dim_company`

| Column | Notes |
|---|---|
| `company_key` | hash of `cik` |
| `cik` | natural key |
| `company_name` | latest from `silver.filing` |
| `ticker` | joined from `configs/sp500.csv` |
| `updated_at` | pipeline run timestamp |

**Rows: 499 (100% ticker coverage)**

### `gold.dim_filing`

| Column | Notes |
|---|---|
| `filing_key` | hash of `accession_number` |
| `accession_number` | natural key |
| `cik` | |
| `form_type`, `filing_date`, `fiscal_year`, `fiscal_period`, `report_date` | |
| `is_amendment` | derived: `form_type LIKE '%-A'` |

**Rows: 18,227**

### `gold.dim_reporting_period`

| Column | Notes |
|---|---|
| `period_key` | hash of `(period_end, fiscal_period, period_type)` |
| `period_end` | natural key component |
| `fiscal_year`, `fiscal_period`, `period_type` | |
| `calendar_year`, `calendar_quarter` | derived from `period_end` |

**Rows: 6,666**

### `gold.dim_metric`

Static seed table — 10 rows, one per canonical metric. Written on pipeline init.

| Column | Notes |
|---|---|
| `metric_key` | hash of `metric_name` |
| `metric_name` | natural key |
| `unit` | `USD` or `USD/shares` |
| `statement_type` | `income`, `balance_sheet`, `cash_flow`, `per_share` |

### `gold.fact_financial_metric`

Central fact table. One row per unique financial observation.

| Column | Notes |
|---|---|
| `fact_key` | hash of `(cik, accession_number, tag, period_end, fiscal_period, form_type)` |
| `company_key`, `filing_key`, `period_key`, `metric_key` | FK to dims |
| `cik`, `accession_number`, `metric_name` | degenerate dims — filter without join |
| `value` | DOUBLE |
| `source_hash` | lineage |

Partitioned by `metric_name` for query performance.  
**Rows: 399,482 | Orphan keys: 0 | Null values: 0**

### `gold.fact_narrative`

One row per extracted section per filing.

| Column | Notes |
|---|---|
| `narrative_key` | hash of `(cik, accession_number, item_name)` |
| `company_key`, `filing_key` | FK to dims |
| `cik`, `accession_number` | degenerate dims |
| `form_type`, `filing_date`, `item_name` | |
| `text_content` | full section text |
| `char_count`, `source_hash` | |

Partitioned by `item_name`.

---

## Repository Structure

```
sec-filing-analytics/
├── configs/
│   ├── pipeline.yml          # project-wide config (rates, universe, storage limits)
│   └── sp500.csv             # S&P 500 membership with CIK and ticker
├── notebooks/
│   ├── bronze/               # ingest_submissions, ingest_company_facts
│   ├── silver/               # transform_filings, transform_observations, extract_narratives
│   ├── gold/                 # build_dimensions, build_facts, build_narratives
│   └── profiling/            # silver_data_quality, gold_data_quality
├── src/sec_filing_analytics/
│   ├── bronze/               # schema, submissions, company_facts
│   ├── silver/               # schema, filings, observations, narratives
│   ├── gold/                 # schema, dimensions, facts, narratives
│   ├── sec_client/           # HTTP client with rate limiting and retry
│   └── universe/             # S&P 500 membership loader
└── tests/unit/               # 25 tests, all passing
```

---

## Key Engineering Principles Applied

| Principle | Implementation |
|---|---|
| **Idempotency** | All writes use Delta MERGE on natural keys — safe to re-run |
| **Deterministic keys** | SHA-256 hash surrogate keys — same key every run |
| **Lineage** | `source_hash` and `accession_number` traceable from Gold back to Bronze |
| **No silent failures** | Warnings logged per skipped payload; `coalesce(key, 0)` for unresolved FKs |
| **EPS isolation** | `USD/shares` metrics handled separately, never mixed into USD totals |
| **Deduplication** | Two-pass window dedupe in Silver; fact table windowed on `filed_date desc` |
| **Partitioning** | `fact_financial_metric` by `metric_name`, `fact_narrative` by `item_name` |
| **CIK normalisation** | All CIKs zero-padded to 10 digits across all layers |

---

## What's Next

### 1. Azure SQL — Serving Layer
Export curated Gold views to Azure SQL Database for BI tool access and interview demos.
- Thin exports: pre-aggregated company financial timeseries
- Read-only connection from Power BI / Tableau
- Options: `spark-mssql-connector` or Databricks JDBC export job

### 2. Analytics / Serving Views
Create Gold views on top of the fact tables for common query patterns:
- `vw_company_financials` — annual/quarterly metrics per company pivoted
- `vw_financial_ratios` — computed ratios (P/E, ROE, debt-to-equity) if market data added
- `vw_narrative_search` — full-text searchable risk factor text

### 3. `orphan_filings` Investigation
`fact_financial_metric` shows 147,622 rows with `filing_key = 0` — meaning observations from `silver.observation` don't match any `dim_filing` accession number. This is because `silver.observation` rows can have empty `accession_number` (observations sourced from XBRL company-facts that pre-date formal accession tracking). Not a data error, but worth documenting.

### 4. Narrative Backfill
`silver.filing_narrative` is currently sparse — the `extract_narratives` task fetches live from SEC EDGAR (~30 min full run). Run once to populate. `gold.fact_narrative` will populate automatically on the next pipeline run after.
