import logging
import re

_log = logging.getLogger(__name__)


def get_azure_sql_connection_properties(dbutils, scope: str = "azure_sql") -> dict:
    return {
        "host": dbutils.secrets.get(scope=scope, key="server"),
        "port": "1433",
        "database": dbutils.secrets.get(scope=scope, key="database"),
        "user": dbutils.secrets.get(scope=scope, key="username"),
        "password": dbutils.secrets.get(scope=scope, key="password"),
        "encrypt": "true",
        "trustServerCertificate": "false",
    }


def export_table_to_azure_sql(df, target_table: str, conn_props: dict, batch_size: int = 10000):
    (
        df.write
        .format("sqlserver")
        .option("host", conn_props["host"])
        .option("port", conn_props["port"])
        .option("database", conn_props["database"])
        .option("dbtable", target_table)
        .option("user", conn_props["user"])
        .option("password", conn_props["password"])
        .option("encrypt", conn_props["encrypt"])
        .option("trustServerCertificate", conn_props["trustServerCertificate"])
        .option("batchsize", str(batch_size))
        .mode("append")
        .save()
    )


def validate_export_columns(dataframe_columns: list[str], sql_columns: list[str], target_table: str) -> None:
    """Fail before truncation if a Gold table cannot fit the serving table schema."""
    source = {column.casefold() for column in dataframe_columns}
    target = {column.casefold() for column in sql_columns}
    if source != target:
        missing = sorted(source - target)
        unexpected = sorted(target - source)
        raise ValueError(
            f"Schema mismatch for {target_table}: missing SQL columns={missing}; "
            f"columns absent from Gold={unexpected}"
        )


def truncate_sql_table_after_schema_check(spark, df, target_table: str, conn_props: dict) -> list[str]:
    if not re.fullmatch(r"dbo\.[A-Za-z_][A-Za-z0-9_]*", target_table):
        raise ValueError(f"Unsupported Azure SQL table name: {target_table}")

    jvm = spark._sc._gateway.jvm
    url = f"jdbc:sqlserver://{conn_props['host']}:{conn_props['port']};databaseName={conn_props['database']};encrypt={conn_props['encrypt']};trustServerCertificate={conn_props['trustServerCertificate']}"
    conn = jvm.java.sql.DriverManager.getConnection(url, conn_props["user"], conn_props["password"])
    try:
        stmt = conn.createStatement()
        rs = stmt.executeQuery(f"SELECT TOP (0) * FROM {target_table}")
        metadata = rs.getMetaData()
        sql_columns = [metadata.getColumnName(i) for i in range(1, metadata.getColumnCount() + 1)]
        validate_export_columns(df.columns, sql_columns, target_table)
        rs.close()
        stmt.execute(f"TRUNCATE TABLE {target_table}")
        stmt.close()
        return sql_columns
    finally:
        conn.close()


def select_tables_to_export(tables: list[tuple[str, str, str]], table_names: list[str] | None):
    if table_names is None:
        return tables
    if not table_names:
        raise ValueError("At least one Azure SQL export table must be selected")
    requested = set(table_names)
    available = {name for name, _, _ in tables}
    unknown = requested - available
    if unknown:
        raise ValueError(f"Unknown Azure SQL export table(s): {sorted(unknown)}")
    return [entry for entry in tables if entry[0] in requested]


def execute_azure_sql_ddl(spark, conn_props: dict, ddl_statements: list[str]):
    jvm = spark._sc._gateway.jvm
    url = f"jdbc:sqlserver://{conn_props['host']}:{conn_props['port']};databaseName={conn_props['database']};encrypt={conn_props['encrypt']};trustServerCertificate={conn_props['trustServerCertificate']}"
    conn = jvm.java.sql.DriverManager.getConnection(url, conn_props["user"], conn_props["password"])
    try:
        stmt = conn.createStatement()
        for ddl in ddl_statements:
            clean_ddl = ddl.strip()
            if clean_ddl:
                stmt.execute(clean_ddl)
        stmt.close()
    finally:
        conn.close()


def export_gold_to_azure_sql(
    spark,
    dbutils,
    catalog: str = "sec_analytics",
    scope: str = "azure_sql",
    table_names: list[str] | None = None,
) -> dict:
    from sec_filing_analytics.serving.schema import get_all_serving_ddls, get_serving_migration_ddls

    conn_props = get_azure_sql_connection_properties(dbutils, scope)

    if hasattr(spark, "_sc") and spark._sc is not None:
        try:
            execute_azure_sql_ddl(spark, conn_props, get_all_serving_ddls() + get_serving_migration_ddls())
        except Exception as exc:
            _log.exception("Azure SQL schema setup failed")
            raise RuntimeError("Azure SQL schema setup failed; export was not started") from exc
    else:
        raise RuntimeError("Azure SQL schema setup requires an active Spark JVM gateway")

    tables_to_export = [
        ("dim_company",           f"{catalog}.gold.dim_company",           "dbo.dim_company"),
        ("dim_filing",            f"{catalog}.gold.dim_filing",            "dbo.dim_filing"),
        ("dim_reporting_period",  f"{catalog}.gold.dim_reporting_period",  "dbo.dim_reporting_period"),
        ("dim_metric",            f"{catalog}.gold.dim_metric",            "dbo.dim_metric"),
        ("fact_financial_metric", f"{catalog}.gold.fact_financial_metric", "dbo.fact_financial_metric"),
        ("fact_narrative",        f"{catalog}.gold.fact_narrative",        "dbo.fact_narrative"),
        ("fact_corporate_event",  f"{catalog}.gold.fact_corporate_event",  "dbo.fact_corporate_event"),
    ]
    tables_to_export = select_tables_to_export(tables_to_export, table_names)

    results = {}
    for name, src, dest in tables_to_export:
        df = spark.table(src)
        cnt = df.count()
        sql_columns = truncate_sql_table_after_schema_check(spark, df, dest, conn_props)
        # Azure SQL ALTER TABLE appends migrated columns to the physical schema;
        # align write order with SQL metadata before connector append mode.
        export_table_to_azure_sql(df.select(*sql_columns), dest, conn_props)
        results[name] = cnt

    return results
