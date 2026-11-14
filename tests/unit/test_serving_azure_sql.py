
from unittest.mock import MagicMock
from sec_filing_analytics.serving.azure_sql import get_azure_sql_connection_properties
from sec_filing_analytics.serving.schema import get_all_serving_ddls


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
    assert len(ddls) == 6

    combined = "\n".join(ddls)
    assert "dbo.dim_company" in combined
    assert "dbo.dim_filing" in combined
    assert "sec_url" in combined
    assert "dbo.dim_reporting_period" in combined
    assert "dbo.dim_metric" in combined
    assert "dbo.fact_financial_metric" in combined
    assert "dbo.fact_narrative" in combined
    assert "dbo.vw_company_annual_financials" not in combined

