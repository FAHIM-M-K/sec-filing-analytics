def get_azure_sql_connection_properties(dbutils, scope: str = "azure_sql") -> dict:
    server = dbutils.secrets.get(scope=scope, key="server")
    database = dbutils.secrets.get(scope=scope, key="database")
    username = dbutils.secrets.get(scope=scope, key="username")
    password = dbutils.secrets.get(scope=scope, key="password")

    jdbc_url = (
        f"jdbc:sqlserver://{server}:1433;"
        f"database={database};"
        f"encrypt=true;"
        f"trustServerCertificate=false;"
        f"hostNameInCertificate=*.database.windows.net;"
        f"loginTimeout=30;"
    )
    return {
        "url": jdbc_url,
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }


def export_table_to_azure_sql(df, target_table: str, conn_props: dict, batch_size: int = 10000):
    (
        df.write
        .format("jdbc")
        .option("url", conn_props["url"])
        .option("dbtable", target_table)
        .option("user", conn_props["user"])
        .option("password", conn_props["password"])
        .option("driver", conn_props["driver"])
        .option("batchsize", batch_size)
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
