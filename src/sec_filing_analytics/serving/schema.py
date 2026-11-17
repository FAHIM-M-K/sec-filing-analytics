"""Azure SQL serving layer schema definitions and DDL statements."""


def azure_sql_dim_company_ddl() -> str:
    return """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_company' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.dim_company (
        company_key  BIGINT NOT NULL PRIMARY KEY,
        cik          VARCHAR(10) NOT NULL,
        company_name VARCHAR(255) NULL,
        ticker       VARCHAR(10) NULL,
        updated_at   DATETIME2 NULL
    );
    CREATE NONCLUSTERED INDEX ix_company_ticker ON dbo.dim_company (ticker);
END;
"""


def azure_sql_dim_filing_ddl() -> str:
    return """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_filing' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.dim_filing (
        filing_key       BIGINT NOT NULL PRIMARY KEY,
        accession_number VARCHAR(25) NOT NULL,
        cik              VARCHAR(10) NOT NULL,
        form_type        VARCHAR(20) NULL,
        filing_date      VARCHAR(10) NULL,
        fiscal_year      INT NULL,
        fiscal_period    VARCHAR(10) NULL,
        report_date      VARCHAR(10) NULL,
        is_amendment     BIT NULL,
        primary_document VARCHAR(255) NULL,
        sec_url          VARCHAR(500) NULL
    );
    CREATE NONCLUSTERED INDEX ix_filing_cik ON dbo.dim_filing (cik);
END;
"""


def azure_sql_dim_period_ddl() -> str:
    return """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_reporting_period' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.dim_reporting_period (
        period_key       BIGINT NOT NULL PRIMARY KEY,
        period_start     VARCHAR(10) NULL,
        period_end       VARCHAR(10) NOT NULL,
        fiscal_year      INT NULL,
        fiscal_period    VARCHAR(10) NULL,
        period_type      VARCHAR(20) NULL,
        calendar_year    INT NULL,
        calendar_quarter INT NULL
    );
    CREATE NONCLUSTERED INDEX ix_period_fiscal_year ON dbo.dim_reporting_period (fiscal_year);
END;
"""


def azure_sql_dim_metric_ddl() -> str:
    return """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_metric' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.dim_metric (
        metric_key     BIGINT NOT NULL PRIMARY KEY,
        metric_name    VARCHAR(100) NOT NULL,
        unit           VARCHAR(50) NULL,
        statement_type VARCHAR(50) NULL
    );
    CREATE NONCLUSTERED INDEX ix_metric_name ON dbo.dim_metric (metric_name);
END;
"""


def azure_sql_dim_period_migration_ddl() -> str:
    return """
IF COL_LENGTH('dbo.dim_reporting_period', 'period_start') IS NULL
    ALTER TABLE dbo.dim_reporting_period ADD period_start VARCHAR(10) NULL;
"""


def azure_sql_fact_financial_metric_ddl() -> str:
    return """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_financial_metric' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.fact_financial_metric (
        fact_key         BIGINT NOT NULL PRIMARY KEY,
        company_key      BIGINT NOT NULL,
        filing_key       BIGINT NOT NULL,
        period_key       BIGINT NOT NULL,
        metric_key       BIGINT NOT NULL,
        cik              VARCHAR(10) NOT NULL,
        accession_number VARCHAR(25) NOT NULL,
        metric_name      VARCHAR(100) NOT NULL,
        value            FLOAT NULL,
        source_hash      VARCHAR(64) NULL
    );
    CREATE NONCLUSTERED INDEX ix_fact_company ON dbo.fact_financial_metric (company_key);
    CREATE NONCLUSTERED INDEX ix_fact_period  ON dbo.fact_financial_metric (period_key);
    CREATE NONCLUSTERED INDEX ix_fact_metric  ON dbo.fact_financial_metric (metric_key);
END;
"""


def azure_sql_fact_narrative_ddl() -> str:
    return """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_narrative' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.fact_narrative (
        narrative_key    BIGINT NOT NULL PRIMARY KEY,
        company_key      BIGINT NOT NULL,
        filing_key       BIGINT NOT NULL,
        cik              VARCHAR(10) NOT NULL,
        accession_number VARCHAR(25) NOT NULL,
        form_type        VARCHAR(20) NULL,
        filing_date      VARCHAR(10) NULL,
        item_name        VARCHAR(50) NOT NULL,
        text_content     NVARCHAR(MAX) NULL,
        char_count       INT NULL,
        source_hash      VARCHAR(64) NULL
    );
    CREATE NONCLUSTERED INDEX ix_narrative_company ON dbo.fact_narrative (company_key);
    CREATE NONCLUSTERED INDEX ix_narrative_filing  ON dbo.fact_narrative (filing_key);
    CREATE NONCLUSTERED INDEX ix_narrative_item    ON dbo.fact_narrative (item_name);
END;
"""


def azure_sql_fact_corporate_event_ddl() -> str:
    return """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_corporate_event' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.fact_corporate_event (
        event_key        BIGINT NOT NULL PRIMARY KEY,
        company_key      BIGINT NOT NULL,
        cik              VARCHAR(10) NOT NULL,
        accession_number VARCHAR(25) NOT NULL,
        form_type        VARCHAR(20) NOT NULL,
        filing_date      VARCHAR(10) NULL,
        item_code        VARCHAR(10) NOT NULL,
        item_description VARCHAR(100) NULL,
        event_headline   NVARCHAR(500) NULL,
        event_text       NVARCHAR(MAX) NULL,
        char_count       INT NULL,
        source_hash      VARCHAR(64) NULL
    );
    CREATE NONCLUSTERED INDEX ix_event_company ON dbo.fact_corporate_event (company_key);
    CREATE NONCLUSTERED INDEX ix_event_date    ON dbo.fact_corporate_event (filing_date);
    CREATE NONCLUSTERED INDEX ix_event_item    ON dbo.fact_corporate_event (item_code);
END;
"""


def get_all_serving_ddls() -> list[str]:
    return [
        azure_sql_dim_company_ddl(),
        azure_sql_dim_filing_ddl(),
        azure_sql_dim_period_ddl(),
        azure_sql_dim_metric_ddl(),
        azure_sql_fact_financial_metric_ddl(),
        azure_sql_fact_narrative_ddl(),
        azure_sql_fact_corporate_event_ddl(),
    ]


def get_serving_ddls_for_tables(table_names: list[str]) -> list[str]:
    ddl_by_table = {
        "dim_company": azure_sql_dim_company_ddl,
        "dim_filing": azure_sql_dim_filing_ddl,
        "dim_reporting_period": azure_sql_dim_period_ddl,
        "dim_metric": azure_sql_dim_metric_ddl,
        "fact_financial_metric": azure_sql_fact_financial_metric_ddl,
        "fact_narrative": azure_sql_fact_narrative_ddl,
        "fact_corporate_event": azure_sql_fact_corporate_event_ddl,
    }
    unknown = set(table_names) - ddl_by_table.keys()
    if unknown:
        raise ValueError(f"Unknown Azure SQL schema table(s): {sorted(unknown)}")
    return [ddl_by_table[name]() for name in ddl_by_table if name in table_names]


def get_serving_migration_ddls() -> list[str]:
    return [azure_sql_dim_period_migration_ddl()]


def get_serving_migrations_for_tables(table_names: list[str]) -> list[str]:
    return [azure_sql_dim_period_migration_ddl()] if "dim_reporting_period" in table_names else []
