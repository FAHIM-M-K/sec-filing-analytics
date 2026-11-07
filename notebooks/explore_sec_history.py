# Databricks notebook source
# MAGIC %md
# MAGIC # Explore SEC history and filing data
# MAGIC
# MAGIC Use Apple as one example to inspect older filing history, selected financial facts since 2010, and Item 1A text in two 10-K documents. This notebook only reads SEC data and prints small samples.

# COMMAND ----------

import json
import re
import time

import requests


CIK = "0000320193"  # Apple Inc.
HEADERS = {"User-Agent": "sec-filing-analytics thisimyid1239@gmail.com"}


def get_sec(url):
    time.sleep(0.2)  # Space these sequential requests below the SEC rate limit.
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response


def filing_rows(data):
    # The current response nests rows under filings.recent; history files store them at the top level.
    columns = data.get("filings", {}).get("recent", data)
    count = len(columns.get("form", []))
    fields = ("accessionNumber", "filingDate", "reportDate", "form", "primaryDocument")
    return [
        {
            field: columns[field][i] if i < len(columns.get(field, [])) else None
            for field in fields
        }
        for i in range(count)
    ]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Older filing history
# MAGIC
# MAGIC The current Submissions response points to separate files for older filings. This cell fetches the referenced file and compares the filing rows around 2010 onward.

# COMMAND ----------

submissions_response = get_sec(f"https://data.sec.gov/submissions/CIK{CIK}.json")
submissions = submissions_response.json()
history_file = submissions["filings"]["files"][0]
history_url = f"https://data.sec.gov/submissions/{history_file['name']}"
history = get_sec(history_url).json()

current_rows = filing_rows(submissions)
older_rows = filing_rows(history)
all_rows = {row["accessionNumber"]: row for row in current_rows + older_rows}
all_rows = list(all_rows.values())
filings_since_2010 = [
    row for row in all_rows
    if row["filingDate"] and row["filingDate"] >= "2010-01-01"
]
annual_and_quarterly = [
    row for row in filings_since_2010
    if row["form"] in ("10-K", "10-K/A", "10-Q", "10-Q/A")
]

print("Referenced history file:", history_file["name"])
print("Date range in referenced file:", history_file["filingFrom"], "to", history_file["filingTo"])
print("Rows in current response:", len(current_rows))
print("Rows in history file:", len(older_rows))
print("Unique filings dated 2010 onward:", len(filings_since_2010))
print("10-K and 10-Q filings dated 2010 onward:", len(annual_and_quarterly))
print("First 5 relevant filings by filing date:")
print(json.dumps(sorted(annual_and_quarterly, key=lambda row: row["filingDate"])[:5], indent=2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## A few financial facts since 2010
# MAGIC
# MAGIC These are examples of commonly used company-wide facts. We keep the SEC tag, unit, dates, form, and accession visible because facts with the same period can come from different filings.

# COMMAND ----------

facts_response = get_sec(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json")
company_facts = facts_response.json()
us_gaap = company_facts["facts"].get("us-gaap", {})

fact_tags = (
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "NetIncomeLoss",
    "Assets",
    "Liabilities",
)

for tag in fact_tags:
    if tag not in us_gaap:
        print("Tag not present:", tag)
        continue

    fact = us_gaap[tag]
    unit, observations = next(iter(fact["units"].items()))
    observations_since_2010 = [
        observation for observation in observations
        if observation.get("end", "") >= "2010-01-01"
        and observation.get("form") in ("10-K", "10-K/A", "10-Q", "10-Q/A")
    ]
    observations_since_2010.sort(key=lambda observation: (observation.get("end", ""), observation.get("filed", "")))
    sample = observations_since_2010[:2] + observations_since_2010[-2:]

    print("\nTag:", tag, "| Label:", fact.get("label"), "| Unit:", unit)
    print("Observations since 2010:", len(observations_since_2010))
    print(json.dumps(sample, indent=2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Item 1A in a 2010 and a recent 10-K
# MAGIC
# MAGIC This prints short raw-HTML snippets around the first matches. A match may be from a contents page; this is for inspection, not section extraction.

# COMMAND ----------

ten_ks = [
    row for row in all_rows
    if row["form"] == "10-K" and row["reportDate"] and row["primaryDocument"]
]
filing_2010 = next((row for row in ten_ks if row["reportDate"].startswith("2010-")), None)
latest_10k = max(ten_ks, key=lambda row: row["filingDate"])

if filing_2010 is None:
    raise ValueError("No 2010 10-K was found in the current and historical Submissions data.")

for filing in (filing_2010, latest_10k):
    accession_path = filing["accessionNumber"].replace("-", "")
    url = (
        f"https://www.sec.gov/Archives/edgar/data/{int(CIK)}/"
        f"{accession_path}/{filing['primaryDocument']}"
    )
    response = get_sec(url)
    html = response.text

    print("\n10-K report date:", filing["reportDate"], "| filed:", filing["filingDate"])
    print("Document:", filing["primaryDocument"], "| size:", len(response.content), "bytes")
    position = html.lower().find("item 1a")
    if position == -1:
        print("No literal 'Item 1A' text found in the raw HTML.")
        continue

    for match_number in range(3):
        if position == -1:
            break
        print("\nItem 1A match", match_number + 1, "at character", position)
        print(html[max(0, position - 160):position + 300])
        position = html.lower().find("item 1a", position + 1)
