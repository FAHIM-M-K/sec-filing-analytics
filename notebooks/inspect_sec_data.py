# Databricks notebook source
# MAGIC %md
# MAGIC # Inspect SEC source data
# MAGIC
# MAGIC Look at Apple's Submissions, Company Facts, and one 10-K response.
# MAGIC This is a read-only exploration notebook; it does not write tables or files.

# COMMAND ----------

import json
import time

import requests


CIK = "0000320193"  # Apple Inc.
HEADERS = {"User-Agent": "sec-filing-analytics thisimyid1239@gmail.com"}


def get_sec(url):
    time.sleep(0.2)  # Space these sequential requests below the SEC rate limit.
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response

# COMMAND ----------
# MAGIC %md
# MAGIC ## Company Submissions

# COMMAND ----------

submissions_response = get_sec(f"https://data.sec.gov/submissions/CIK{CIK}.json")
submissions = submissions_response.json()
recent = submissions["filings"]["recent"]

print("Company:", submissions["name"])
print("Top-level fields:", list(submissions))
print("Response size:", len(submissions_response.content), "bytes")
print("Recent filings (first 8):")
print(json.dumps([
    {key: recent[key][i] for key in (
        "accessionNumber", "filingDate", "reportDate", "form", "primaryDocument", "fy", "fp"
    )}
    for i in range(min(8, len(recent["form"])))
], indent=2))
print("Older filing-history files:", submissions["filings"].get("files", []))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Company Facts

# COMMAND ----------

facts_response = get_sec(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json")
company_facts = facts_response.json()
us_gaap = company_facts["facts"].get("us-gaap", {})

print("Entity:", company_facts["entityName"])
print("Taxonomies:", list(company_facts["facts"]))
print("Number of us-gaap tags:", len(us_gaap))
print("Response size:", len(facts_response.content), "bytes")
for tag in list(us_gaap)[:3]:
    print("\n", tag, "—", us_gaap[tag]["label"])
    unit, observations = next(iter(us_gaap[tag]["units"].items()))
    print("Unit:", unit)
    print(json.dumps(observations[:2], indent=2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## One 10-K filing document
# MAGIC
# MAGIC The preview below is raw HTML. We are inspecting the source shape, not extracting sections yet.

# COMMAND ----------

ten_k = next(i for i, form in enumerate(recent["form"]) if form == "10-K")
accession = recent["accessionNumber"][ten_k]
document = recent["primaryDocument"][ten_k]
filing_url = (
    f"https://www.sec.gov/Archives/edgar/data/{int(CIK)}/"
    f"{accession.replace('-', '')}/{document}"
)
filing_response = get_sec(filing_url)

print("Filing date:", recent["filingDate"][ten_k])
print("Document:", document)
print("Content type:", filing_response.headers.get("Content-Type"))
print("Response size:", len(filing_response.content), "bytes")
print("Position of Item 1A text:", filing_response.text.lower().find("item 1a"))
print("HTML preview:\n", filing_response.text[:3000])
