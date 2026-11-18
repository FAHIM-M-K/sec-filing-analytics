from sec_filing_analytics.silver.observations import _parse_facts_data


def test_silver_observation_canonical_tag_mapping():
    """Verify raw US-GAAP XBRL concepts map to canonical metrics and pre-2010 data is filtered out."""
    payload = {
        "cik": "320193",
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {
                        "USD": [
                            {
                                "accn": "0000320193-24-000123",
                                "form": "10-K",
                                "fp": "FY",
                                "fy": 2024,
                                "filed": "2024-11-01",
                                "start": "2023-10-01",
                                "end": "2024-09-28",
                                "val": 391035000000,
                            },
                            {
                                "accn": "0000320193-09-000050",
                                "form": "10-K",
                                "fp": "FY",
                                "fy": 2009,
                                "filed": "2009-10-27",
                                "start": "2008-09-28",
                                "end": "2009-09-26",
                                "val": 36537000000,
                            },
                        ]
                    }
                },
                "Assets": {
                    "units": {
                        "USD": [
                            {
                                "accn": "0000320193-24-000123",
                                "form": "10-K",
                                "fp": "FY",
                                "fy": 2024,
                                "filed": "2024-11-01",
                                "end": "2024-09-28",
                                "val": 364980000000,
                            }
                        ]
                    }
                },
            }
        },
    }

    rows = _parse_facts_data(payload, "hash_lineage_123")

    assert all(r["period_end"] >= "2010-01-01" for r in rows)
    revenue = next(r for r in rows if r["metric"] == "Revenue")
    assets = next(r for r in rows if r["metric"] == "Total Assets")

    assert revenue["cik"] == "0000320193"
    assert revenue["period_type"] == "duration"
    assert revenue["value"] == 391035000000.0

    assert assets["period_type"] == "instant"
    assert assets["value"] == 364980000000.0


def test_silver_observation_combines_revenue_components():
    """Verify split revenue components (GoodsNet + ServicesNet) are summed into a single GAAP revenue observation."""
    payload = {
        "cik": "1",
        "facts": {
            "us-gaap": {
                "SalesRevenueGoodsNet": {
                    "units": {
                        "USD": [
                            {
                                "accn": "accn-1",
                                "form": "10-K",
                                "fp": "FY",
                                "fy": 2024,
                                "filed": "2025-02-01",
                                "start": "2024-01-01",
                                "end": "2024-12-31",
                                "val": 100.0,
                            }
                        ]
                    }
                },
                "SalesRevenueServicesNet": {
                    "units": {
                        "USD": [
                            {
                                "accn": "accn-1",
                                "form": "10-K",
                                "fp": "FY",
                                "fy": 2024,
                                "filed": "2025-02-01",
                                "start": "2024-01-01",
                                "end": "2024-12-31",
                                "val": 25.0,
                            }
                        ]
                    }
                },
            }
        },
    }

    rows = _parse_facts_data(payload, "hash_lineage_456")
    revenue_rows = [r for r in rows if r["metric"] == "Revenue"]

    assert len(revenue_rows) == 1
    assert revenue_rows[0]["tag"] == "SalesRevenueGoodsNet+SalesRevenueServicesNet"
    assert revenue_rows[0]["value"] == 125.0
