import csv
import os
import logging
from datetime import datetime, timezone

_log = logging.getLogger(__name__)

_METRIC_CATALOG = [
    {"metric_name": "Revenue",             "unit": "USD",        "statement_type": "income"},
    {"metric_name": "Net Income",          "unit": "USD",        "statement_type": "income"},
    {"metric_name": "Operating Income",    "unit": "USD",        "statement_type": "income"},
    {"metric_name": "Operating Cash Flow", "unit": "USD",        "statement_type": "cash_flow"},
    {"metric_name": "EPS Diluted",         "unit": "USD/shares", "statement_type": "per_share"},
    {"metric_name": "EPS Basic",           "unit": "USD/shares", "statement_type": "per_share"},
    {"metric_name": "Total Assets",        "unit": "USD",        "statement_type": "balance_sheet"},
    {"metric_name": "Total Liabilities",   "unit": "USD",        "statement_type": "balance_sheet"},
    {"metric_name": "Cash",                "unit": "USD",        "statement_type": "balance_sheet"},
    {"metric_name": "Long-term Debt",      "unit": "USD",        "statement_type": "balance_sheet"},
]


def _hash_key(F, *col_exprs):
    from pyspark.sql.types import LongType
    return F.conv(
        F.substring(F.sha2(F.concat_ws("|", *col_exprs), 256), 1, 15),
        16, 10,
    ).cast(LongType())


def _load_sp500_ticker_map(sp500_path: str) -> dict[str, str]:
    ticker_map: dict[str, str] = {}
    if not os.path.exists(sp500_path):
        _log.warning("sp500.csv not found at %s — tickers will be null", sp500_path)
        return ticker_map
    with open(sp500_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cik = row["CIK"].strip().zfill(10)
            ticker = row["Ticker"].strip()
            if cik and ticker:
                ticker_map[cik] = ticker
    return ticker_map


def build_dim_company(spark, catalog: str, sp500_path: str):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from sec_filing_analytics.gold.schema import GOLD_DIM_COMPANY_SCHEMA

    ticker_map = _load_sp500_ticker_map(sp500_path)

    filing = spark.table(f"{catalog}.silver.filing")
    latest = (
        filing
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("cik").orderBy(F.col("filing_date").desc())
        ))
        .filter(F.col("_rn") == 1)
        .select("cik", "company_name")
    )

    now = datetime.now(timezone.utc)
    ticker_map_bc = spark.sparkContext.broadcast(ticker_map)

    @F.udf("string")
    def _ticker(cik):
        return ticker_map_bc.value.get(cik)

    df = (
        latest
        .withColumn("ticker",      _ticker(F.col("cik")))
        .withColumn("updated_at",  F.lit(now).cast("timestamp"))
        .withColumn("company_key", _hash_key(F, F.col("cik")))
        .select([fld.name for fld in GOLD_DIM_COMPANY_SCHEMA])
    )
    return df


def build_dim_filing(spark, catalog: str):
    from pyspark.sql import functions as F
    from sec_filing_analytics.gold.schema import GOLD_DIM_FILING_SCHEMA

    filing = spark.table(f"{catalog}.silver.filing")
    df = (
        filing
        .withColumn("is_amendment", F.col("form_type").rlike(r"/A$"))
        .withColumn("filing_key",   _hash_key(F, F.col("accession_number")))
        .select([fld.name for fld in GOLD_DIM_FILING_SCHEMA])
    )
    return df


def build_dim_period(spark, catalog: str):
    from pyspark.sql import functions as F
    from sec_filing_analytics.gold.schema import GOLD_DIM_PERIOD_SCHEMA

    obs = spark.table(f"{catalog}.silver.observation")
    df = (
        obs
        .select("period_end", "fiscal_year", "fiscal_period", "period_type")
        .distinct()
        .withColumn("calendar_year",    F.year(F.to_date(F.col("period_end"))))
        .withColumn("calendar_quarter", F.quarter(F.to_date(F.col("period_end"))))
        .withColumn("period_key", _hash_key(F, F.col("period_end"), F.col("fiscal_period"), F.col("period_type")))
        .select([fld.name for fld in GOLD_DIM_PERIOD_SCHEMA])
    )
    return df


def build_dim_metric(spark):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    from sec_filing_analytics.gold.schema import GOLD_DIM_METRIC_SCHEMA

    seed_schema = StructType([
        StructField("metric_name",    StringType(), False),
        StructField("unit",           StringType(), True),
        StructField("statement_type", StringType(), True),
    ])
    df = spark.createDataFrame(_METRIC_CATALOG, schema=seed_schema)
    df = (
        df
        .withColumn("metric_key", _hash_key(F, F.col("metric_name")))
        .select([fld.name for fld in GOLD_DIM_METRIC_SCHEMA])
    )
    return df


def upsert_dim_company(spark, df, catalog: str) -> dict:
    from delta.tables import DeltaTable
    tbl = f"{catalog}.gold.dim_company"
    (
        DeltaTable.forName(spark, tbl).alias("t")
        .merge(df.alias("s"), "t.cik = s.cik")
        .whenMatchedUpdate(set={
            "company_name": "s.company_name",
            "ticker":       "s.ticker",
            "updated_at":   "s.updated_at",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"total_rows": spark.table(tbl).count()}


def upsert_dim_filing(spark, df, catalog: str) -> dict:
    from delta.tables import DeltaTable
    tbl = f"{catalog}.gold.dim_filing"
    (
        DeltaTable.forName(spark, tbl).alias("t")
        .merge(df.alias("s"), "t.accession_number = s.accession_number")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"total_rows": spark.table(tbl).count()}


def upsert_dim_period(spark, df, catalog: str) -> dict:
    from delta.tables import DeltaTable
    tbl = f"{catalog}.gold.dim_reporting_period"
    (
        DeltaTable.forName(spark, tbl).alias("t")
        .merge(
            df.alias("s"),
            "t.period_end = s.period_end AND t.fiscal_period = s.fiscal_period AND t.period_type = s.period_type",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"total_rows": spark.table(tbl).count()}


def upsert_dim_metric(spark, df, catalog: str) -> dict:
    from delta.tables import DeltaTable
    tbl = f"{catalog}.gold.dim_metric"
    (
        DeltaTable.forName(spark, tbl).alias("t")
        .merge(df.alias("s"), "t.metric_name = s.metric_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"total_rows": spark.table(tbl).count()}
