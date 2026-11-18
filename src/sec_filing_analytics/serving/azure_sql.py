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


def execute_sql_script(spark, conn_props: dict, sql_script: str, transactional: bool = False) -> None:
    """Execute raw SQL statements on Azure SQL Database via JVM JDBC driver."""
    if hasattr(spark, "_sc") and spark._sc is not None:
        jvm = spark._sc._gateway.jvm
        url = (
            f"jdbc:sqlserver://{conn_props['host']}:{conn_props['port']};"
            f"databaseName={conn_props['database']};"
            f"encrypt={conn_props['encrypt']};"
            f"trustServerCertificate={conn_props['trustServerCertificate']}"
        )
        conn = jvm.java.sql.DriverManager.getConnection(url, conn_props["user"], conn_props["password"])
        try:
            if transactional:
                conn.setAutoCommit(False)
            stmt = conn.createStatement()
            stmt.execute(sql_script.strip())
            if transactional:
                conn.commit()
            stmt.close()
        except Exception:
            if transactional:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            conn.close()


def build_atomic_swap_sql(tables_to_swap: list[tuple[str, str, str]]) -> str:
    """Generate transactional SQL script that truncates production tables and swaps in validated staging rows."""
    swap_clauses = []
    for _, _, dest in tables_to_swap:
        stg_dest = dest.replace("dbo.", "dbo.stg_")
        swap_clauses.append(f"TRUNCATE TABLE {dest};")
        swap_clauses.append(f"INSERT INTO {dest} SELECT * FROM {stg_dest};")

    return (
        "BEGIN TRANSACTION;\n"
        "BEGIN TRY\n"
        + "\n".join("    " + clause for clause in swap_clauses)
        + "\n    COMMIT TRANSACTION;\n"
        "END TRY\n"
        "BEGIN CATCH\n"
        "    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;\n"
        "    THROW;\n"
        "END CATCH;"
    )


def ensure_staging_tables_exist(spark, conn_props: dict, tables_to_export: list[tuple[str, str, str]]) -> None:
    """Ensure each destination table has a matching dbo.stg_<name> table with identical schema."""
    for _, _, dest in tables_to_export:
        stg_dest = dest.replace("dbo.", "dbo.stg_")
        ddl = f"""
        IF OBJECT_ID('{stg_dest}', 'U') IS NOT NULL
        BEGIN
            IF (SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('{stg_dest}')) <>
               (SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('{dest}'))
                DROP TABLE {stg_dest};
        END;
        IF OBJECT_ID('{stg_dest}', 'U') IS NULL
            SELECT TOP (0) * INTO {stg_dest} FROM {dest};
        """
        try:
            execute_sql_script(spark, conn_props, ddl, transactional=False)
        except Exception as exc:
            _log.warning("Could not ensure staging table %s: %s", stg_dest, exc)


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
        .option("truncate", "true")
        .mode("overwrite")
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


def validate_sql_table_schema(spark, df, target_table: str, conn_props: dict) -> list[str]:
    if not re.fullmatch(r"dbo\.[A-Za-z_][A-Za-z0-9_]*", target_table):
        raise ValueError(f"Unsupported Azure SQL table name: {target_table}")
    target_schema = (
        spark.read.format("sqlserver")
        .options(**{
            "host": conn_props["host"],
            "port": conn_props["port"],
            "database": conn_props["database"],
            "user": conn_props["user"],
            "password": conn_props["password"],
            "encrypt": conn_props["encrypt"],
            "trustServerCertificate": conn_props["trustServerCertificate"],
            "query": f"SELECT TOP (0) * FROM {target_table}",
        })
        .load()
    )
    sql_columns = target_schema.columns
    validate_export_columns(df.columns, sql_columns, target_table)
    return sql_columns


def validate_sql_row_count(spark, target_table: str, expected_rows: int, conn_props: dict) -> None:
    if not re.fullmatch(r"dbo\.[A-Za-z_][A-Za-z0-9_]*", target_table):
        raise ValueError(f"Unsupported Azure SQL table name: {target_table}")
    actual_rows = (
        spark.read.format("sqlserver")
        .options(**{
            "host": conn_props["host"],
            "port": conn_props["port"],
            "database": conn_props["database"],
            "user": conn_props["user"],
            "password": conn_props["password"],
            "encrypt": conn_props["encrypt"],
            "trustServerCertificate": conn_props["trustServerCertificate"],
            "query": f"SELECT COUNT_BIG(*) AS row_count FROM {target_table}",
        })
        .load()
        .first()["row_count"]
    )
    if int(actual_rows) != expected_rows:
        raise RuntimeError(
            f"Azure SQL row-count mismatch for {target_table}: "
            f"expected {expected_rows:,}, found {int(actual_rows):,}."
        )


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


def export_gold_to_azure_sql(
    spark,
    dbutils,
    catalog: str = "sec_analytics",
    scope: str = "azure_sql",
    table_names: list[str] | None = None,
    atomic: bool = True,
) -> dict:
    from sec_filing_analytics.serving.schema import get_serving_migrations_for_tables

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

    conn_props = get_azure_sql_connection_properties(dbutils, scope)

    # 1. Apply any pending idempotent migrations on destination tables
    selected_names = [name for name, _, _ in tables_to_export]
    for migration_sql in get_serving_migrations_for_tables(selected_names):
        try:
            execute_sql_script(spark, conn_props, migration_sql, transactional=False)
        except Exception as exc:
            _log.warning("Migration skipped or already present: %s", exc)

    # 2. Validate schemas before modifying any data
    prepared = []
    results = {}
    for name, src, dest in tables_to_export:
        df = spark.table(src)
        cnt = df.count()
        if name in {"dim_reporting_period", "fact_financial_metric"} and cnt == 0:
            raise RuntimeError(f"Gold source {src} is empty; refusing to truncate {dest}.")
        sql_columns = validate_sql_table_schema(spark, df, dest, conn_props)
        prepared.append((name, df.select(*sql_columns), dest, cnt))

    if atomic:
        # 3a. Staging & Transactional Atomic Swap Pattern
        ensure_staging_tables_exist(spark, conn_props, tables_to_export)

        for name, df, dest, cnt in prepared:
            stg_dest = dest.replace("dbo.", "dbo.stg_")
            try:
                export_table_to_azure_sql(df, stg_dest, conn_props)
                validate_sql_row_count(spark, stg_dest, cnt, conn_props)
            except Exception as exc:
                raise RuntimeError(
                    f"Staging failed for {stg_dest}; production tables remain untouched: {exc}"
                ) from exc

        # All staging tables are loaded and validated: swap atomically inside one SQL transaction
        swap_sql = build_atomic_swap_sql(tables_to_export)
        execute_sql_script(spark, conn_props, swap_sql, transactional=True)

        for name, _, dest, cnt in prepared:
            validate_sql_row_count(spark, dest, cnt, conn_props)
            results[name] = cnt

    else:
        # 3b. Sequential overwrite fallback
        completed = []
        for name, df, dest, cnt in prepared:
            try:
                export_table_to_azure_sql(df, dest, conn_props)
                validate_sql_row_count(spark, dest, cnt, conn_props)
            except Exception as exc:
                published = ", ".join(completed) or "none"
                raise RuntimeError(
                    f"Azure SQL export stopped at {dest}; row counts for completed tables "
                    f"were validated ({published}). The selected tables may now be from mixed "
                    "refreshes; rerun the full selected export before refreshing Power BI."
                ) from exc
            results[name] = cnt
            completed.append(name)

    return results
