import re


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
) -> dict:
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

    prepared = []
    results = {}
    for name, src, dest in tables_to_export:
        df = spark.table(src)
        cnt = df.count()
        if name in {"dim_reporting_period", "fact_financial_metric"} and cnt == 0:
            raise RuntimeError(f"Gold source {src} is empty; refusing to truncate {dest}.")
        sql_columns = validate_sql_table_schema(spark, df, dest, conn_props)
        prepared.append((name, df.select(*sql_columns), dest, cnt))

    # Do not begin any overwrite until every selected source and destination
    # schema has passed validation.
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
