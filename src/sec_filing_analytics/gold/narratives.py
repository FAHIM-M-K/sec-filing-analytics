import logging

_log = logging.getLogger(__name__)


def _hash_key(F, *col_exprs):
    from pyspark.sql.types import LongType
    return F.conv(
        F.substring(F.sha2(F.concat_ws("|", *col_exprs), 256), 1, 15),
        16, 10,
    ).cast(LongType())


def build_fact_narrative(spark, catalog: str):
    from pyspark.sql import functions as F
    from sec_filing_analytics.gold.schema import GOLD_FACT_NARRATIVE_SCHEMA

    narrative = spark.table(f"{catalog}.silver.filing_narrative")
    company   = spark.table(f"{catalog}.gold.dim_company").select("cik", "company_key")
    filing    = spark.table(f"{catalog}.gold.dim_filing").select("accession_number", "filing_key")

    df = (
        narrative
        .join(company, on="cik",              how="left")
        .join(filing,  on="accession_number", how="left")
        .withColumn("company_key", F.coalesce(F.col("company_key"), F.lit(0).cast("long")))
        .withColumn("filing_key",  F.coalesce(F.col("filing_key"),  F.lit(0).cast("long")))
        .withColumn(
            "narrative_key",
            _hash_key(F, F.col("cik"), F.col("accession_number"), F.col("item_name")),
        )
        .select([fld.name for fld in GOLD_FACT_NARRATIVE_SCHEMA])
    )
    return df


def upsert_fact_narrative(spark, df, catalog: str) -> dict:
    from delta.tables import DeltaTable

    tbl = f"{catalog}.gold.fact_narrative"
    merge_condition = (
        "t.cik = s.cik AND "
        "t.accession_number = s.accession_number AND "
        "t.item_name = s.item_name"
    )
    (
        DeltaTable.forName(spark, tbl).alias("t")
        .merge(df.alias("s"), merge_condition)
        .whenMatchedUpdate(set={
            "text_content":  "s.text_content",
            "char_count":    "s.char_count",
            "source_hash":   "s.source_hash",
            "narrative_key": "s.narrative_key",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"total_rows": spark.table(tbl).count()}

