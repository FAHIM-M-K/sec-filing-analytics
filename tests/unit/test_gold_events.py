from sec_filing_analytics.gold.schema import (
    GOLD_FACT_CORPORATE_EVENT_SCHEMA,
    fact_corporate_event_ddl,
)
from sec_filing_analytics.silver.events import TARGET_8K_ITEMS


def test_gold_fact_corporate_event_schema_fields():
    field_names = [f.name for f in GOLD_FACT_CORPORATE_EVENT_SCHEMA]
    assert "event_key" in field_names
    assert "company_key" in field_names
    assert "cik" in field_names
    assert "accession_number" in field_names
    assert "item_code" in field_names
    assert "item_description" in field_names
    assert "event_headline" in field_names
    assert "event_text" in field_names
    assert "char_count" in field_names
    assert "source_hash" in field_names


def test_gold_fact_corporate_event_ddl():
    ddl = fact_corporate_event_ddl("sec_analytics")
    assert "CREATE TABLE IF NOT EXISTS sec_analytics.gold.fact_corporate_event" in ddl
    assert "PARTITIONED BY (item_code)" in ddl
    assert "event_key         BIGINT NOT NULL" in ddl


def test_target_8k_items_taxonomy():
    assert "4.02" in TARGET_8K_ITEMS
    assert "5.02" in TARGET_8K_ITEMS
    assert "1.01" in TARGET_8K_ITEMS
    assert "2.05" in TARGET_8K_ITEMS
    assert "5.07" not in TARGET_8K_ITEMS  # Routine shareholder vote excluded
    assert "7.01" not in TARGET_8K_ITEMS  # Routine slide deck excluded
