IF OBJECT_ID('dbo.dim_company', 'U') IS NULL
BEGIN
    ;THROW 50001, 'dbo.dim_company must exist before applying this migration.', 1;
END;

IF COL_LENGTH('dbo.dim_company', 'is_current_sp500') IS NULL
BEGIN
    ALTER TABLE dbo.dim_company
    ADD is_current_sp500 BIT NOT NULL
        CONSTRAINT DF_dim_company_is_current_sp500 DEFAULT (1);
END;
