
from unittest.mock import MagicMock
import pytest

from sec_filing_analytics.serving.azure_sql import (
    get_azure_sql_connection_properties,
    select_tables_to_export,
    validate_export_columns,
)
from sec_filing_analytics.serving.schema import get_all_serving_ddls, get_serving_migration_ddls


def test_get_azure_sql_connection_properties():
    mock_dbutils = MagicMock()
    mock_dbutils.secrets.get.side_effect = lambda scope, key: {
        "server": "test-server.database.windows.net",
        "database": "test-db",
        "username": "test-user",
        "password": "test-password",
    }[key]

    props = get_azure_sql_connection_properties(mock_dbutils, scope="azure_sql")
    assert props["host"] == "test-server.database.windows.net"
    assert props["database"] == "test-db"
    assert props["user"] == "test-user"
    assert props["password"] == "test-password"
    assert props["port"] == "1433"
    assert props["encrypt"] == "true"


def test_serving_schema_ddls():
    ddls = get_all_serving_ddls()
    assert len(ddls) == 7

    combined = "\n".join(ddls)
    assert "dbo.dim_company" in combined
    assert "dbo.dim_filing" in combined
    assert "sec_url" in combined
    assert "dbo.dim_reporting_period" in combined
    assert "dbo.dim_metric" in combined
    assert "dbo.fact_financial_metric" in combined
    assert "dbo.fact_narrative" in combined
    assert "dbo.fact_corporate_event" in combined
    assert "period_start" in combined
    assert "dbo.vw_company_annual_financials" not in combined


def test_period_start_migration_is_idempotent():
    ddl = "\n".join(get_serving_migration_ddls())
    assert "COL_LENGTH('dbo.dim_reporting_period', 'period_start') IS NULL" in ddl
    assert "ADD period_start VARCHAR(10) NULL" in ddl


def test_export_schema_accepts_matching_columns_case_insensitively():
    validate_export_columns(["period_key", "period_start"], ["PERIOD_KEY", "PERIOD_START"], "dbo.dim_reporting_period")


def test_export_schema_rejects_mismatch_before_table_truncation():
    with pytest.raises(ValueError, match="Schema mismatch for dbo.dim_reporting_period"):
        validate_export_columns(["period_key", "period_start"], ["period_key"], "dbo.dim_reporting_period")


def test_export_schema_allows_physical_column_order_differences():
    # SQL Server migrations append new columns; the exporter reorders the frame to match.
    validate_export_columns(["period_key", "period_start"], ["period_start", "period_key"], "dbo.dim_reporting_period")


def test_export_table_selection_keeps_unrelated_tables_out_of_rebuild():
    tables = [
        ("dim_reporting_period", "gold.dim_reporting_period", "dbo.dim_reporting_period"),
        ("fact_financial_metric", "gold.fact_financial_metric", "dbo.fact_financial_metric"),
        ("fact_narrative", "gold.fact_narrative", "dbo.fact_narrative"),
    ]
    selected = select_tables_to_export(tables, ["dim_reporting_period", "fact_financial_metric"])
    assert [entry[0] for entry in selected] == ["dim_reporting_period", "fact_financial_metric"]


def test_export_table_selection_rejects_unknown_table_names():
    with pytest.raises(ValueError, match="Unknown Azure SQL export table"):
        select_tables_to_export([], ["dbo.users"])

