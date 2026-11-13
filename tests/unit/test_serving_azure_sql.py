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
    assert props["host"] == "test-server.database.windows.net"
    assert props["database"] == "test-db"
    assert props["user"] == "test-user"
    assert props["password"] == "test-password"
    assert props["port"] == "1433"
    assert props["encrypt"] == "true"
