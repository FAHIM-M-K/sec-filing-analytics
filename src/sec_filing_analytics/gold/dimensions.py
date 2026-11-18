from datetime import datetime, timezone

from sec_filing_analytics.gold.schema import _hash_key

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


def _load_sp500_universe(sp500_path: str) -> list[dict]:
    from sec_filing_analytics.universe.membership import load_membership_csv

    return [
        {
            "cik": company["cik"],
            "ticker": company["ticker"],
            "csv_name": company["company_name"],
            "in_roster": True,
        }
        for company in load_membership_csv(sp500_path)
    ]


def build_dim_company(spark, catalog: str, sp500_path: str):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from sec_filing_analytics.gold.schema import GOLD_DIM_COMPANY_SCHEMA

    univ_rows = _load_sp500_universe(sp500_path)
    univ_df = spark.createDataFrame(univ_rows)

    filing = spark.table(f"{catalog}.silver.filing")
    latest = (
        filing
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("cik").orderBy(F.col("filing_date").desc(), F.col("accession_number").desc())
        ))
        .filter(F.col("_rn") == 1)
        .select("cik", F.col("company_name").alias("filing_name"))
    )

    existing = spark.table(f"{catalog}.gold.dim_company").select(
        "cik",
        F.col("company_name").alias("previous_name"),
        F.col("ticker").alias("previous_ticker"),
    )
    known_ciks = (
        univ_df.select("cik")
        .unionByName(latest.select("cik"))
        .unionByName(existing.select("cik"))
        .filter(F.col("cik").isNotNull())
        .distinct()
    )

    now = datetime.now(timezone.utc)

    df = (
        known_ciks
        .join(univ_df, on="cik", how="left")
        .join(latest, on="cik", how="left")
        .join(existing, on="cik", how="left")
        .withColumn(
            "company_name",
            F.coalesce(F.col("filing_name"), F.col("csv_name"), F.col("previous_name"), F.col("ticker"), F.lit("Unknown")),
        )
        .withColumn("ticker", F.coalesce(F.col("ticker"), F.col("previous_ticker")))
        .withColumn("updated_at",  F.lit(now).cast("timestamp"))
        .withColumn("company_key", _hash_key(F, F.col("cik")))
        .withColumn("is_current_sp500", F.coalesce(F.col("in_roster"), F.lit(False)))
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
        .withColumn(
            "sec_url",
            F.when(
                F.col("primary_document").isNotNull(),
                F.concat(
                    F.lit("https://www.sec.gov/Archives/edgar/data/"),
                    F.regexp_replace(F.col("cik"), r"^0+", ""),
                    F.lit("/"),
                    F.regexp_replace(F.col("accession_number"), r"-", ""),
                    F.lit("/"),
                    F.col("primary_document"),
                ),
            ).otherwise(
                F.concat(
                    F.lit("https://www.sec.gov/edgar/browse/?CIK="),
                    F.regexp_replace(F.col("cik"), r"^0+", ""),
                )
            ),
        )
        .select([fld.name for fld in GOLD_DIM_FILING_SCHEMA])
    )

    # Ingest historical filings referenced in observations but absent from submissions.
    obs = spark.table(f"{catalog}.silver.observation")
    existing_accns = filing.select("accession_number").distinct()
    missing_filings = (
        obs
        .filter(F.col("accession_number").isNotNull() & (F.col("accession_number") != ""))
        .join(existing_accns, on="accession_number", how="left_anti")
        .groupBy("accession_number", "cik")
        .agg(
            F.first("form_type").alias("form_type"),
            F.first("filed_date").alias("filing_date"),
            F.max_by("fiscal_year", "period_end").alias("fiscal_year"),
            F.max_by("fiscal_period", "period_end").alias("fiscal_period"),
            F.max("period_end").alias("report_date"),
        )
        .withColumn("is_amendment", F.col("form_type").rlike(r"/A$"))
        .withColumn("primary_document", F.lit(None).cast("string"))
        .withColumn("filing_key", _hash_key(F, F.col("accession_number")))
        .withColumn(
            "sec_url",
            F.concat(
                F.lit("https://www.sec.gov/edgar/browse/?CIK="),
                F.regexp_replace(F.col("cik"), r"^0+", ""),
            )
        )
        .select([fld.name for fld in GOLD_DIM_FILING_SCHEMA])
    )
    df = df.unionByName(missing_filings)

    sentinel = spark.createDataFrame(
        [{"filing_key": 0, "accession_number": "UNKNOWN", "cik": "UNKNOWN",
          "form_type": "HISTORICAL/UNTRACKED", "filing_date": None,
          "fiscal_year": None, "fiscal_period": None, "report_date": None,
          "is_amendment": False, "primary_document": None, "sec_url": None}],
        schema=GOLD_DIM_FILING_SCHEMA,
    )
    return df.union(sentinel)


def build_dim_period(spark, catalog: str):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from sec_filing_analytics.gold.schema import GOLD_DIM_PERIOD_SCHEMA

    obs = spark.table(f"{catalog}.silver.observation")
    w = Window.partitionBy("period_start", "period_end", "fiscal_period", "period_type").orderBy(F.col("fiscal_year").desc_nulls_last())
    df = (
        obs
        .select("period_start", "period_end", "fiscal_year", "fiscal_period", "period_type")
        .distinct()
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("calendar_year",    F.year(F.to_date(F.col("period_end"))))
        .withColumn("calendar_quarter", F.quarter(F.to_date(F.col("period_end"))))
        .withColumn("period_key", _hash_key(F, F.col("period_start"), F.col("period_end"), F.col("fiscal_period"), F.col("period_type")))
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
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable
    tbl = f"{catalog}.gold.dim_company"
    (
        DeltaTable.forName(spark, tbl).alias("t")
        .merge(df.alias("s"), "t.cik = s.cik")
        .whenMatchedUpdate(set={
            "company_name": "s.company_name",
            "ticker":       "s.ticker",
            "updated_at":   "s.updated_at",
            "is_current_sp500": "s.is_current_sp500",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    result = spark.table(tbl).agg(
        F.sum(F.col("is_current_sp500").cast("int")).alias("current_rows")
    ).first()
    total_rows = spark.table(tbl).count()
    return {
        "total_rows": total_rows,
        "current_rows": result["current_rows"] or 0,
        "inactive_rows": total_rows - (result["current_rows"] or 0),
    }


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
