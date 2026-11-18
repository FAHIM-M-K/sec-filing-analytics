from sec_filing_analytics.gold.schema import _hash_key


def build_fact_financial_metric(spark, catalog: str):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from sec_filing_analytics.gold.schema import GOLD_FACT_SCHEMA

    obs     = spark.table(f"{catalog}.silver.observation")
    company = spark.table(f"{catalog}.gold.dim_company").select("cik", "company_key")
    filing  = spark.table(f"{catalog}.gold.dim_filing").select("accession_number", "filing_key")
    period  = spark.table(f"{catalog}.gold.dim_reporting_period").alias("p")
    metric  = spark.table(f"{catalog}.gold.dim_metric").select("metric_name", "metric_key")

    observation = obs.alias("o")
    period_match = (
        F.col("o.period_start").eqNullSafe(F.col("p.period_start"))
        & F.col("o.period_end").eqNullSafe(F.col("p.period_end"))
        & F.col("o.fiscal_period").eqNullSafe(F.col("p.fiscal_period"))
        & F.col("o.period_type").eqNullSafe(F.col("p.period_type"))
    )
    observation = (
        observation
        .join(period, period_match, how="left")
        .select("o.*", F.col("p.period_key").alias("period_key"))
        .withColumnRenamed("metric", "metric_name")
    )

    df = (
        observation
        .join(company, on="cik",              how="left")
        .join(filing,  on="accession_number", how="left")
        .join(metric,  on="metric_name",      how="left")
        .withColumnRenamed("tag", "source_tag")
        .withColumn(
            "fact_key",
            _hash_key(F, F.col("cik"), F.col("accession_number"), F.col("source_tag"),
                      F.col("period_start"), F.col("period_end"), F.col("fiscal_period"), F.col("form_type")),
        )
        .withColumn("company_key", F.coalesce(F.col("company_key"), F.lit(0).cast("long")))
        .withColumn("filing_key",  F.coalesce(F.col("filing_key"),  F.lit(0).cast("long")))
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

def upsert_fact_financial_metric(spark, df, catalog: str = "sec_analytics") -> dict:
    from delta.tables import DeltaTable
    from sec_filing_analytics.gold.schema import fact_ddl

    spark.sql(fact_ddl(catalog))
    table_name = f"{catalog}.gold.fact_financial_metric"

    # If the target Delta table does not exist or has 0 rows, initialize with partition
    try:
        target = DeltaTable.forName(spark, table_name)
    except Exception:
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("metric_name")
            .saveAsTable(table_name)
        )
        return {"total_rows": spark.table(table_name).count(), "operation": "initial_load"}

    merge_condition = (
        "t.cik = s.cik AND "
        "t.accession_number = s.accession_number AND "
        "t.metric_name = s.metric_name AND "
        "t.period_key = s.period_key"
    )

    (
        target.alias("t")
        .merge(df.alias("s"), merge_condition)
        .whenMatchedUpdate(set={
            "fact_key":    "s.fact_key",
            "company_key": "s.company_key",
            "filing_key":  "s.filing_key",
            "metric_key":  "s.metric_key",
            "source_tag":  "s.source_tag",
            "value":       "s.value",
            "source_hash": "s.source_hash",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    total = spark.table(table_name).count()
    return {"total_rows": total, "operation": "merge"}
