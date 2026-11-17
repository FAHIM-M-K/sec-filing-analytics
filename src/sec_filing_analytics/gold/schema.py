try:
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, LongType, IntegerType, DoubleType, BooleanType, TimestampType,
    )
except ImportError:
    class _StubType:
        def __init__(self, *args, **kwargs):
            self.name = kwargs.get("name", args[0] if args and isinstance(args[0], str) else "")
            self.fields = args[0] if args and isinstance(args[0], list) else []
            self.dataType = self
        def simpleString(self):
            return "string"
        def __iter__(self):
            return iter(self.fields)
        def __call__(self, *args, **kwargs):
            return self
    StructType = StructField = StringType = LongType = IntegerType = DoubleType = BooleanType = TimestampType = _StubType

GOLD_DIM_COMPANY_SCHEMA = StructType([
    StructField("company_key",  LongType(),      False),
    StructField("cik",          StringType(),    False),
    StructField("company_name", StringType(),    True),
    StructField("ticker",       StringType(),    True),
    StructField("updated_at",   TimestampType(), True),
])

GOLD_DIM_FILING_SCHEMA = StructType([
    StructField("filing_key",        LongType(),   False),
    StructField("accession_number",  StringType(), False),
    StructField("cik",               StringType(), False),
    StructField("form_type",         StringType(), True),
    StructField("filing_date",       StringType(), True),
    StructField("fiscal_year",       IntegerType(), True),
    StructField("fiscal_period",     StringType(), True),
    StructField("report_date",       StringType(), True),
    StructField("is_amendment",      BooleanType(), True),
    StructField("primary_document",  StringType(), True),
    StructField("sec_url",           StringType(), True),
])

GOLD_DIM_PERIOD_SCHEMA = StructType([
    StructField("period_key",      LongType(),    False),
    StructField("period_start",    StringType(),  True),
    StructField("period_end",      StringType(),  False),
    StructField("fiscal_year",     IntegerType(), True),
    StructField("fiscal_period",   StringType(),  True),
    StructField("period_type",     StringType(),  True),
    StructField("calendar_year",   IntegerType(), True),
    StructField("calendar_quarter", IntegerType(), True),
])

GOLD_DIM_METRIC_SCHEMA = StructType([
    StructField("metric_key",      LongType(),   False),
    StructField("metric_name",     StringType(), False),
    StructField("unit",            StringType(), True),
    StructField("statement_type",  StringType(), True),
])

GOLD_FACT_SCHEMA = StructType([
    StructField("fact_key",          LongType(),   False),
    StructField("company_key",       LongType(),   False),
    StructField("filing_key",        LongType(),   False),
    StructField("period_key",        LongType(),   False),
    StructField("metric_key",        LongType(),   False),
    StructField("cik",               StringType(), False),
    StructField("accession_number",  StringType(), False),
    StructField("metric_name",       StringType(), False),
    StructField("value",             DoubleType(), True),
    StructField("source_hash",       StringType(), True),
])

GOLD_FACT_NARRATIVE_SCHEMA = StructType([
    StructField("narrative_key",     LongType(),    False),
    StructField("company_key",       LongType(),    False),
    StructField("filing_key",        LongType(),    False),
    StructField("cik",               StringType(),  False),
    StructField("accession_number",  StringType(),  False),
    StructField("form_type",         StringType(),  True),
    StructField("filing_date",       StringType(),  True),
    StructField("item_name",         StringType(),  False),
    StructField("text_content",      StringType(),  True),
    StructField("char_count",        IntegerType(), True),
    StructField("source_hash",       StringType(),  True),
])

GOLD_FACT_CORPORATE_EVENT_SCHEMA = StructType([
    StructField("event_key",         LongType(),    False),
    StructField("company_key",       LongType(),    False),
    StructField("cik",               StringType(),  False),
    StructField("accession_number",  StringType(),  False),
    StructField("form_type",         StringType(),  False),
    StructField("filing_date",       StringType(),  True),
    StructField("item_code",         StringType(),  False),
    StructField("item_description",  StringType(),  True),
    StructField("event_headline",    StringType(),  True),
    StructField("event_text",        StringType(),  True),
    StructField("char_count",        IntegerType(), True),
    StructField("source_hash",       StringType(),  True),
])


def dim_company_ddl(catalog: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_company (
    company_key  BIGINT NOT NULL,
    cik          STRING NOT NULL,
    company_name STRING,
    ticker       STRING,
    updated_at   TIMESTAMP
)
USING DELTA
"""


def dim_filing_ddl(catalog: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_filing (
    filing_key       BIGINT NOT NULL,
    accession_number STRING NOT NULL,
    cik              STRING NOT NULL,
    form_type        STRING,
    filing_date      STRING,
    fiscal_year      INT,
    fiscal_period    STRING,
    report_date      STRING,
    is_amendment     BOOLEAN,
    primary_document STRING,
    sec_url          STRING
)
USING DELTA
"""


def dim_period_ddl(catalog: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_reporting_period (
    period_key       BIGINT NOT NULL,
    period_start     STRING,
    period_end       STRING NOT NULL,
    fiscal_year      INT,
    fiscal_period    STRING,
    period_type      STRING,
    calendar_year    INT,
    calendar_quarter INT
)
USING DELTA
"""


def dim_metric_ddl(catalog: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_metric (
    metric_key     BIGINT NOT NULL,
    metric_name    STRING NOT NULL,
    unit           STRING,
    statement_type STRING
)
USING DELTA
"""


def fact_ddl(catalog: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_financial_metric (
    fact_key         BIGINT NOT NULL,
    company_key      BIGINT NOT NULL,
    filing_key       BIGINT NOT NULL,
    period_key       BIGINT NOT NULL,
    metric_key       BIGINT NOT NULL,
    cik              STRING NOT NULL,
    accession_number STRING NOT NULL,
    metric_name      STRING NOT NULL,
    value            DOUBLE,
    source_hash      STRING
)
USING DELTA
PARTITIONED BY (metric_name)
"""


def fact_narrative_ddl(catalog: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_narrative (
    narrative_key    BIGINT NOT NULL,
    company_key      BIGINT NOT NULL,
    filing_key       BIGINT NOT NULL,
    cik              STRING NOT NULL,
    accession_number STRING NOT NULL,
    form_type        STRING,
    filing_date      STRING,
    item_name        STRING NOT NULL,
    text_content     STRING,
    char_count       INT,
    source_hash      STRING
)
USING DELTA
PARTITIONED BY (item_name)
"""


def fact_corporate_event_ddl(catalog: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_corporate_event (
    event_key         BIGINT NOT NULL,
    company_key       BIGINT NOT NULL,
    cik               STRING NOT NULL,
    accession_number  STRING NOT NULL,
    form_type         STRING NOT NULL,
    filing_date       STRING,
    item_code         STRING NOT NULL,
    item_description  STRING,
    event_headline    STRING,
    event_text        STRING,
    char_count        INT,
    source_hash       STRING
)
USING DELTA
PARTITIONED BY (item_code)
"""
