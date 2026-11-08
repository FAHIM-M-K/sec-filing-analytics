"""PySpark schemas and DDL helpers for Bronze Delta manifest tables."""

from __future__ import annotations

from typing import Any

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SUBMISSION_MANIFEST_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("cik", StringType(), nullable=False),
        StructField("entity_name", StringType(), nullable=True),
        StructField("fetched_at", TimestampType(), nullable=False),
        StructField("source_url", StringType(), nullable=False),
        StructField("content_hash", StringType(), nullable=False),
        StructField("response_size_bytes", LongType(), nullable=True),
        StructField("payload_path", StringType(), nullable=True),
        StructField("is_new_version", BooleanType(), nullable=False),
        StructField("filing_count_recent", IntegerType(), nullable=True),
        StructField("older_history_file_count", IntegerType(), nullable=True),
    ]
)

COMPANY_FACTS_MANIFEST_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("cik", StringType(), nullable=False),
        StructField("entity_name", StringType(), nullable=True),
        StructField("fetched_at", TimestampType(), nullable=False),
        StructField("source_url", StringType(), nullable=False),
        StructField("content_hash", StringType(), nullable=False),
        StructField("response_size_bytes", LongType(), nullable=True),
        StructField("payload_path", StringType(), nullable=True),
        StructField("is_new_version", BooleanType(), nullable=False),
        StructField("us_gaap_tag_count", IntegerType(), nullable=True),
        StructField("taxonomies", ArrayType(StringType()), nullable=True),
    ]
)


def create_submission_manifest_table(spark: Any, table_name: str) -> None:
    """Create the Submissions manifest Delta table if it does not already exist."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            run_id              STRING    NOT NULL,
            cik                 STRING    NOT NULL,
            entity_name         STRING,
            fetched_at          TIMESTAMP NOT NULL,
            source_url          STRING    NOT NULL,
            content_hash        STRING    NOT NULL,
            response_size_bytes LONG,
            payload_path        STRING,
            is_new_version      BOOLEAN   NOT NULL,
            filing_count_recent INT,
            older_history_file_count INT
        )
        USING DELTA
        COMMENT 'Bronze manifest for SEC Company Submissions fetches. One row per (CIK, content version).'
        """
    )


def create_company_facts_manifest_table(spark: Any, table_name: str) -> None:
    """Create the Company Facts manifest Delta table if it does not already exist."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            run_id              STRING    NOT NULL,
            cik                 STRING    NOT NULL,
            entity_name         STRING,
            fetched_at          TIMESTAMP NOT NULL,
            source_url          STRING    NOT NULL,
            content_hash        STRING    NOT NULL,
            response_size_bytes LONG,
            payload_path        STRING,
            is_new_version      BOOLEAN   NOT NULL,
            us_gaap_tag_count   INT,
            taxonomies          ARRAY<STRING>
        )
        USING DELTA
        COMMENT 'Bronze manifest for SEC Company Facts fetches. One row per (CIK, content version).'
        """
    )

