"""Silver layer schema definitions and DDL helpers."""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType,
)


SILVER_FILING_SCHEMA = StructType([
    StructField("cik",              StringType(),    False),
    StructField("accession_number", StringType(),    False),
    StructField("form_type",        StringType(),    False),
    StructField("filing_date",      StringType(),    True),
    StructField("report_date",      StringType(),    True),
    StructField("fiscal_year",      IntegerType(),   True),
    StructField("fiscal_period",    StringType(),    True),  # FY, Q1, Q2, Q3
    StructField("company_name",     StringType(),    True),
    StructField("primary_document", StringType(),    True),
    StructField("source_hash",      StringType(),    True),
])


SILVER_OBSERVATION_SCHEMA = StructType([
    StructField("cik",            StringType(),  False),
    StructField("accession_number", StringType(), True),
    StructField("tag",            StringType(),  False),
    StructField("metric",         StringType(),  False),  # canonical name
    StructField("period_type",    StringType(),  False),  # instant | duration
    StructField("period_start",   StringType(),  True),   # null for instant
    StructField("period_end",     StringType(),  False),
    StructField("fiscal_year",    IntegerType(), True),
    StructField("fiscal_period",  StringType(),  True),
    StructField("form_type",      StringType(),  True),
    StructField("unit",           StringType(),  False),
    StructField("value",          DoubleType(),  True),
    StructField("filed_date",     StringType(),  True),
    StructField("source_hash",    StringType(),  True),
])


def filing_ddl(catalog: str = "sec_analytics") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.silver.filing (
        cik              STRING  NOT NULL,
        accession_number STRING  NOT NULL,
        form_type        STRING  NOT NULL,
        filing_date      STRING,
        report_date      STRING,
        fiscal_year      INT,
        fiscal_period    STRING,
        company_name     STRING,
        primary_document STRING,
        source_hash      STRING
    )
    USING DELTA
    PARTITIONED BY (form_type)
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')
    """


def observation_ddl(catalog: str = "sec_analytics") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.silver.observation (
        cik              STRING  NOT NULL,
        accession_number STRING,
        tag              STRING  NOT NULL,
        metric           STRING  NOT NULL,
        period_type      STRING  NOT NULL,
        period_start     STRING,
        period_end       STRING  NOT NULL,
        fiscal_year      INT,
        fiscal_period    STRING,
        form_type        STRING,
        unit             STRING  NOT NULL,
        value            DOUBLE,
        filed_date       STRING,
        source_hash      STRING
    )
    USING DELTA
    PARTITIONED BY (metric)
    """

