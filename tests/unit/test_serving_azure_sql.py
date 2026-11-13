from unittest.mock import MagicMock
from sec_filing_analytics.serving.azure_sql import get_azure_sql_connection_properties


def test_get_azure_sql_connection_properties():
    mock_dbutils = MagicMock()
    mock_dbutils.secrets.get.side_effect = lambda scope, key: {
        "server": "test-server.database.windows.net",
        "database": "test-db",
        "username": "test-user",
        "password": "test-password",
    }[key]

    props = get_azure_sql_connection_properties(mock_dbutils, scope="azure_sql")
    assert props["user"] == "test-user"
    assert props["password"] == "test-password"
    assert "jdbc:sqlserver://test-server.database.windows.net:1433" in props["url"]
    assert "database=test-db" in props["url"]
    assert props["driver"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
