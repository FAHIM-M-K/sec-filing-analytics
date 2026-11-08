# Databricks notebook source
# MAGIC %md
# MAGIC # Inspect Item 1A Section Structure Across Filing Eras
# MAGIC
# MAGIC Apple 2010 10-K had zero literal "Item 1A" matches. This notebook investigates why,
# MAGIC by comparing the raw HTML structure of the 2010 and 2026 10-K documents.
# MAGIC Read-only. No tables or files are written.

# COMMAND ----------

import time
import requests

HEADERS = {"User-Agent": "sec-filing-analytics thisimyid1239@gmail.com"}


def get_sec(url):
    time.sleep(0.2)
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response


def search_terms(html, terms):
    """Return all character positions for each search term (case-insensitive)."""
    results = {}
    lower = html.lower()
    for term in terms:
        positions = []
        pos = 0
        while True:
            idx = lower.find(term.lower(), pos)
            if idx == -1:
                break
            positions.append(idx)
            pos = idx + 1
        results[term] = positions
    return results


def show_context(html, position, before=200, after=400, label=""):
    """Print a labelled slice of HTML around a character position."""
    snippet = html[max(0, position - before) : position + after]
    print(f"\n--- {label} (char {position}) ---")
    print(snippet)


# COMMAND ----------
# MAGIC %md
# MAGIC ## Fetch the 2010 Apple 10-K

# COMMAND ----------

CIK_INT = 320193
ACCESSION_2010 = "0001193125-10-238044"
DOCUMENT_2010 = "d10k.htm"

accession_path_2010 = ACCESSION_2010.replace("-", "")
url_2010 = f"https://www.sec.gov/Archives/edgar/data/{CIK_INT}/{accession_path_2010}/{DOCUMENT_2010}"

print("Fetching:", url_2010)
response_2010 = get_sec(url_2010)
html_2010 = response_2010.text

print(f"Content-Type : {response_2010.headers.get('Content-Type')}")
print(f"Response size: {len(response_2010.content):,} bytes")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2010 Document — First 2,000 Characters

# COMMAND ----------

print(html_2010[:2000])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2010 Document — Search for Section Marker Variants

# COMMAND ----------

SEARCH_TERMS = ["item 1a", "item 1a.", "risk factor", "1a.", "1a&#", "1a&nbsp", "item&#160;1a"]

hits_2010 = search_terms(html_2010, SEARCH_TERMS)

print("Search results in 2010 10-K:")
for term, positions in hits_2010.items():
    print(f"  {repr(term):30s}  ->  {len(positions)} match(es)  at positions: {positions[:10]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2010 Document — Context Around First Match of Each Term Found

# COMMAND ----------

for term, positions in hits_2010.items():
    if positions:
        show_context(html_2010, positions[0], before=200, after=400, label=f"2010 | {repr(term)} match 1")
    if len(positions) > 1:
        show_context(html_2010, positions[1], before=200, after=400, label=f"2010 | {repr(term)} match 2")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fetch the 2026 Apple 10-K (Comparison)

# COMMAND ----------

ACCESSION_2026 = "0000320193-26-000026"
DOCUMENT_2026 = "aapl-20260926.htm"

accession_path_2026 = ACCESSION_2026.replace("-", "")
url_2026 = f"https://www.sec.gov/Archives/edgar/data/{CIK_INT}/{accession_path_2026}/{DOCUMENT_2026}"

print("Fetching:", url_2026)
response_2026 = get_sec(url_2026)
html_2026 = response_2026.text

print(f"Content-Type : {response_2026.headers.get('Content-Type')}")
print(f"Response size: {len(response_2026.content):,} bytes")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2026 Document — Same Search Terms for Comparison

# COMMAND ----------

hits_2026 = search_terms(html_2026, SEARCH_TERMS)

print("Search results in 2026 10-K:")
for term, positions in hits_2026.items():
    print(f"  {repr(term):30s}  ->  {len(positions)} match(es)  at positions: {positions[:10]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2026 Document — Context Around Match 3 (Likely the Actual Section)
# MAGIC
# MAGIC From the previous notebook we know match 3 is the real section header.

# COMMAND ----------

if hits_2026.get("item 1a") and len(hits_2026["item 1a"]) >= 3:
    show_context(html_2026, hits_2026["item 1a"][2], before=100, after=600, label="2026 | 'item 1a' match 3 (actual section)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Table
# MAGIC
# MAGIC Print a side-by-side count of every term in both filings.

# COMMAND ----------

all_terms = sorted(set(list(hits_2010.keys()) + list(hits_2026.keys())))
print(f"{'Term':<30}  {'2010 matches':>14}  {'2026 matches':>14}")
print("-" * 62)
for term in all_terms:
    c2010 = len(hits_2010.get(term, []))
    c2026 = len(hits_2026.get(term, []))
    print(f"{repr(term):<30}  {c2010:>14}  {c2026:>14}")

