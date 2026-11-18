from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
)


SILVER_FILING_SCHEMA = StructType([
    StructField("cik",              StringType(),    False),
    StructField("accession_number", StringType(),    False),
    StructField("form_type",        StringType(),    False),
    StructField("filing_date",      StringType(),    True),
    StructField("report_date",      StringType(),    True),
    StructField("fiscal_year",      IntegerType(),   True),
    StructField("fiscal_period",    StringType(),    True),
    StructField("company_name",     StringType(),    True),
    StructField("primary_document", StringType(),    True),
    StructField("source_hash",      StringType(),    True),
])


SILVER_OBSERVATION_SCHEMA = StructType([
    StructField("cik",            StringType(),  False),
    StructField("accession_number", StringType(), True),
    StructField("tag",            StringType(),  False),
    StructField("metric",         StringType(),  False),
    StructField("period_type",    StringType(),  False),
    StructField("period_start",   StringType(),  True),
    StructField("period_end",     StringType(),  False),
    StructField("fiscal_year",    IntegerType(), True),
    StructField("fiscal_period",  StringType(),  True),
    StructField("form_type",      StringType(),  True),
    StructField("unit",           StringType(),  False),
    StructField("value",          DoubleType(),  True),
    StructField("filed_date",     StringType(),  True),
    StructField("source_hash",    StringType(),  True),
])


SILVER_NARRATIVE_SCHEMA = StructType([
    StructField("cik",              StringType(), False),
    StructField("accession_number", StringType(), False),
    StructField("form_type",        StringType(), False),
    StructField("filing_date",      StringType(), True),
    StructField("item_name",        StringType(), False),
    StructField("text_content",     StringType(), True),
    StructField("char_count",       IntegerType(), True),
    StructField("source_hash",      StringType(), True),
])


SILVER_CORPORATE_EVENT_SCHEMA = StructType([
    StructField("cik",              StringType(),  False),
    StructField("accession_number", StringType(),  False),
    StructField("form_type",        StringType(),  False),
    StructField("filing_date",      StringType(),  True),
    StructField("report_date",      StringType(),  True),
    StructField("company_name",     StringType(),  True),
    StructField("item_code",        StringType(),  False),
    StructField("item_description", StringType(),  True),
    StructField("primary_document", StringType(),  True),
    StructField("event_headline",   StringType(),  True),
    StructField("event_text",       StringType(),  True),
    StructField("char_count",       IntegerType(), True),
    StructField("source_hash",      StringType(),  True),
])


# Shared set of SEC filing form types covering annual and quarterly reports.
ANNUAL_AND_QUARTERLY_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}


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


def narrative_ddl(catalog: str = "sec_analytics") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.silver.filing_narrative (
        cik              STRING NOT NULL,
        accession_number STRING NOT NULL,
        form_type        STRING NOT NULL,
        filing_date      STRING,
        item_name        STRING NOT NULL,
        text_content     STRING,
        char_count       INT,
        source_hash      STRING
    )
    USING DELTA
    PARTITIONED BY (form_type)
    """


def narrative_attempt_ddl(catalog: str = "sec_analytics") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.silver.filing_narrative_attempt (
        cik              STRING NOT NULL,
        accession_number STRING NOT NULL,
        status           STRING NOT NULL,
        item_count       INT NOT NULL,
        attempt_count    INT NOT NULL,
        last_attempt_at  TIMESTAMP NOT NULL
    )
    USING DELTA
    """


def corporate_event_ddl(catalog: str = "sec_analytics") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.silver.corporate_event (
        cik              STRING NOT NULL,
        accession_number STRING NOT NULL,
        form_type        STRING NOT NULL,
        filing_date      STRING,
        report_date      STRING,
        company_name     STRING,
        item_code        STRING NOT NULL,
        item_description STRING,
        primary_document STRING,
        event_headline   STRING,
        event_text       STRING,
        char_count       INT,
        source_hash      STRING
    )
    USING DELTA
    PARTITIONED BY (item_code)
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
