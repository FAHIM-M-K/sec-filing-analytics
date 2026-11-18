import re
import pytest

from sec_filing_analytics.gold.schema import (
    GOLD_DIM_COMPANY_SCHEMA,
    GOLD_DIM_FILING_SCHEMA,
    GOLD_DIM_METRIC_SCHEMA,
    GOLD_DIM_PERIOD_SCHEMA,
    GOLD_FACT_CORPORATE_EVENT_SCHEMA,
    GOLD_FACT_NARRATIVE_SCHEMA,
    GOLD_FACT_SCHEMA,
)
from sec_filing_analytics.serving.azure_sql import (
    build_atomic_swap_sql,
    validate_export_columns,
)
from sec_filing_analytics.serving.schema import get_all_serving_ddls


def test_serving_ddl_matches_gold_delta_schema():
    """Verify that every Azure SQL serving table DDL matches the corresponding Gold Delta Lake schema."""
    gold_schemas = {
        "dim_company": GOLD_DIM_COMPANY_SCHEMA,
        "dim_filing": GOLD_DIM_FILING_SCHEMA,
        "dim_reporting_period": GOLD_DIM_PERIOD_SCHEMA,
        "dim_metric": GOLD_DIM_METRIC_SCHEMA,
        "fact_financial_metric": GOLD_FACT_SCHEMA,
        "fact_narrative": GOLD_FACT_NARRATIVE_SCHEMA,
        "fact_corporate_event": GOLD_FACT_CORPORATE_EVENT_SCHEMA,
    }

    for ddl in get_all_serving_ddls():
        match = re.search(r"CREATE TABLE dbo\.(\w+)\s*\((.*?)\);", ddl, re.S)
        assert match, f"Could not parse DDL: {ddl}"
        table_name, body = match.groups()
        sql_columns = re.findall(
            r"^\s*([a-zA-Z_]\w*)\s+(?:BIGINT|VARCHAR|NVARCHAR|DATETIME2|INT|BIT|FLOAT)",
            body,
            re.M | re.I,
        )
        gold_columns = [field.name for field in gold_schemas[table_name].fields]
        assert sql_columns == gold_columns, f"Schema mismatch between Gold and Serving for table: {table_name}"


def test_atomic_swap_and_schema_validation():
    """Verify fail-closed pre-flight column validation and transactional atomic swap SQL generation."""
    # 1. Pre-flight column check fails before touching database
    with pytest.raises(ValueError, match="Schema mismatch for dbo.dim_company"):
        validate_export_columns(["company_key", "unexpected_col"], ["company_key"], "dbo.dim_company")

    # 2. Transactional atomic swap script contains BEGIN TRANSACTION, table truncation, and COMMIT
    tables_to_swap = [
        ("dim_company", "gold.dim_company", "dbo.dim_company"),
        ("fact_financial_metric", "gold.fact_financial_metric", "dbo.fact_financial_metric"),
    ]
    swap_sql = build_atomic_swap_sql(tables_to_swap)

    assert "BEGIN TRANSACTION;" in swap_sql
    assert "TRUNCATE TABLE dbo.dim_company;" in swap_sql
    assert "INSERT INTO dbo.dim_company SELECT * FROM dbo.stg_dim_company;" in swap_sql
    assert "TRUNCATE TABLE dbo.fact_financial_metric;" in swap_sql
    assert "INSERT INTO dbo.fact_financial_metric SELECT * FROM dbo.stg_fact_financial_metric;" in swap_sql
    assert "COMMIT TRANSACTION;" in swap_sql
    assert "ROLLBACK TRANSACTION;" in swap_sql
