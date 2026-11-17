from sec_filing_analytics.silver.events import TARGET_8K_ITEMS


def test_target_8k_items_taxonomy():
    assert "4.02" in TARGET_8K_ITEMS
    assert "5.02" in TARGET_8K_ITEMS
    assert "1.01" in TARGET_8K_ITEMS
    assert "2.05" in TARGET_8K_ITEMS
    assert "5.07" not in TARGET_8K_ITEMS  # Routine shareholder vote excluded
    assert "7.01" not in TARGET_8K_ITEMS  # Routine slide deck excluded


