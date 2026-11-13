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


def export_gold_to_azure_sql(spark, dbutils, catalog: str = "sec_analytics", scope: str = "azure_sql") -> dict:
    conn_props = get_azure_sql_connection_properties(dbutils, scope)

    tables_to_export = [
        ("dim_company",           f"{catalog}.gold.dim_company",           "dbo.dim_company"),
        ("dim_filing",            f"{catalog}.gold.dim_filing",            "dbo.dim_filing"),
        ("dim_reporting_period",  f"{catalog}.gold.dim_reporting_period",  "dbo.dim_reporting_period"),
        ("dim_metric",            f"{catalog}.gold.dim_metric",            "dbo.dim_metric"),
        ("fact_financial_metric", f"{catalog}.gold.fact_financial_metric", "dbo.fact_financial_metric"),
    ]

    results = {}
    for name, src, dest in tables_to_export:
        df = spark.table(src)
        cnt = df.count()
        export_table_to_azure_sql(df, dest, conn_props)
        results[name] = cnt

    return results
