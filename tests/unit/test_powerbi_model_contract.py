from pathlib import Path
import re
from datetime import date


MODEL_DIR = Path("sec_analytics_dashboard.SemanticModel/definition/tables")


def test_revenue_ltm_contract_requires_full_quarter_window_from_2010():
    model = (MODEL_DIR / "fact_financial_metric.tmdl").read_text(encoding="utf-8")
    measure = model.split("measure 'Revenue (LTM)' = ```", 1)[1].split("```", 1)[0]

    assert "dim_reporting_period[period_start_date] >= DATE(2010, 1, 1)" in measure
    assert "DATEDIFF(dim_reporting_period[period_start_date], dim_reporting_period[period_end_date], DAY) >= 60" in measure
    assert "DATEDIFF(dim_reporting_period[period_start_date], dim_reporting_period[period_end_date], DAY) <= 120" in measure
    assert "COUNTROWS(QuarterPeriods) = 4" in measure
    assert "QuarterRevenueCount = 4" in measure
    assert "QuarterWindowDays >= 300 && QuarterWindowDays <= 430" in measure
    assert "LatestAnnualFilingKey" in measure
    assert "LatestQuarterFilingKey" in measure
    assert re.search(r"IF\(\s*COUNTROWS\(QuarterPeriods\) = 4.*?\s+QuarterlyLTM,\s+AnnualRevenue\s*\)", measure, re.S)


def test_reporting_period_model_exposes_period_start_date():
    model = (MODEL_DIR / "dim_reporting_period.tmdl").read_text(encoding="utf-8")
    assert "sourceColumn: period_start" in model
    assert "column period_start_date" in model
    assert "sourceColumn: period_start_date" in model
    assert 'Table.AddColumn(dbo_dim_reporting_period, "period_start_date"' in model
    assert "column period_start_date =" not in model


def test_four_quarter_window_spans_fiscal_year_boundary():
    quarter_periods = [
        (date(2023, 10, 1), date(2023, 12, 31), 120),
        (date(2024, 1, 1), date(2024, 3, 31), 130),
        (date(2024, 4, 1), date(2024, 6, 30), 140),
        (date(2024, 7, 1), date(2024, 9, 30), 150),
    ]
    assert all(60 <= (end - start).days <= 120 for start, end, _ in quarter_periods)
    window_days = (quarter_periods[-1][1] - quarter_periods[0][0]).days
    assert 300 <= window_days <= 430
    assert sum(value for _, _, value in quarter_periods) == 540

    model = (MODEL_DIR / "fact_financial_metric.tmdl").read_text(encoding="utf-8")
    measure = model.split("measure 'Revenue (LTM)' = ```", 1)[1].split("```", 1)[0]
    assert re.search(r"TOPN\(\s*4,", measure)
    assert "MINX(QuarterPeriods, [__PeriodStart])" in measure
    assert "MAXX(QuarterPeriods, [__PeriodEnd])" in measure
    assert "QuarterlyLTM" in measure
