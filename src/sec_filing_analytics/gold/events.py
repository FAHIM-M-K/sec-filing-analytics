from __future__ import annotations

import logging
from typing import Any

_log = logging.getLogger(__name__)


def _hash_key(F: Any, *col_exprs: Any) -> Any:
    from pyspark.sql.types import LongType
    return F.conv(
        F.substring(F.sha2(F.concat_ws("|", *col_exprs), 256), 1, 15),
        16, 10,
    ).cast(LongType())


def build_fact_corporate_event(spark: Any, catalog: str) -> Any:
    from pyspark.sql import functions as F
    from sec_filing_analytics.gold.schema import GOLD_FACT_CORPORATE_EVENT_SCHEMA

    events = spark.table(f"{catalog}.silver.corporate_event")
    company = spark.table(f"{catalog}.gold.dim_company").select("cik", "company_key")

    df = (
        events
        .join(company, on="cik", how="left")
        .withColumn("company_key", F.coalesce(F.col("company_key"), F.lit(0).cast("long")))
        .withColumn(
            "event_key",
            _hash_key(F, F.col("cik"), F.col("accession_number"), F.col("item_code")),
        )
        .select([fld.name for fld in GOLD_FACT_CORPORATE_EVENT_SCHEMA])
    )
    return df


def upsert_fact_corporate_event(spark: Any, df: Any, catalog: str) -> dict[str, int]:
    from delta.tables import DeltaTable
    from sec_filing_analytics.gold.schema import fact_corporate_event_ddl

    spark.sql(fact_corporate_event_ddl(catalog))
    tbl = f"{catalog}.gold.fact_corporate_event"
    merge_condition = (
        "t.cik = s.cik AND "
        "t.accession_number = s.accession_number AND "
        "t.item_code = s.item_code"
    )
    (
        DeltaTable.forName(spark, tbl).alias("t")
        .merge(df.alias("s"), merge_condition)
        .whenMatchedUpdate(set={
            "form_type": "s.form_type",
            "filing_date": "s.filing_date",
            "item_description": "s.item_description",
            "event_headline": "s.event_headline",
            "event_text": "s.event_text",
            "char_count": "s.char_count",
            "source_hash": "s.source_hash",
            "event_key": "s.event_key",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"total_rows": spark.table(tbl).count()}
