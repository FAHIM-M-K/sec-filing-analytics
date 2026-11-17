def _hash_key(F, *col_exprs):
    from pyspark.sql.types import LongType
    return F.conv(
        F.substring(F.sha2(F.concat_ws("|", *col_exprs), 256), 1, 15),
        16, 10,
    ).cast(LongType())


def build_fact_financial_metric(spark, catalog: str):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from sec_filing_analytics.gold.schema import GOLD_FACT_SCHEMA

    obs     = spark.table(f"{catalog}.silver.observation")
    company = spark.table(f"{catalog}.gold.dim_company").select("cik", "company_key")
    filing  = spark.table(f"{catalog}.gold.dim_filing").select("accession_number", "filing_key")
    period  = spark.table(f"{catalog}.gold.dim_reporting_period").select(
        "period_start", "period_end", "fiscal_period", "period_type", "period_key"
    )
    metric  = spark.table(f"{catalog}.gold.dim_metric").select("metric_name", "metric_key")

    df = (
        obs
        .withColumnRenamed("metric", "metric_name")
        .join(company, on="cik",              how="left")
        .join(filing,  on="accession_number", how="left")
        .join(period,  on=["period_start", "period_end", "fiscal_period", "period_type"], how="left")
        .join(metric,  on="metric_name",      how="left")
        .withColumn(
            "fact_key",
            _hash_key(F, F.col("cik"), F.col("accession_number"), F.col("tag"),
                      F.col("period_start"), F.col("period_end"), F.col("fiscal_period"), F.col("form_type")),
        )
        .withColumn("company_key", F.coalesce(F.col("company_key"), F.lit(0).cast("long")))
        .withColumn("filing_key",  F.coalesce(F.col("filing_key"),  F.lit(0).cast("long")))
        .withColumn("period_key",  F.coalesce(F.col("period_key"),  F.lit(0).cast("long")))
        .withColumn("metric_key",  F.coalesce(F.col("metric_key"),  F.lit(0).cast("long")))
    )

    w = Window.partitionBy("cik", "accession_number", "metric_name", "period_key").orderBy(
        F.col("filed_date").desc(), F.col("fact_key").asc()
    )
    df = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select([fld.name for fld in GOLD_FACT_SCHEMA])
    )
    return df
