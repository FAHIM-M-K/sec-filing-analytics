# Databricks notebook source

import os, sys
import gzip, json
from pyspark.sql import functions as F

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = "/Workspace" + _nb_path.rsplit("/notebooks", 1)[0]
sys.path.insert(0, os.path.join(_repo_root, "src"))
os.chdir(_repo_root)

from sec_filing_analytics.silver.filings import parse_submission_payload
from sec_filing_analytics.silver.manifests import latest_payload_paths

sub_manifest   = spark.table("sec_analytics.bronze.submission_manifest")

RELEVANT_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}

# COMMAND ----------

latest_submissions = latest_payload_paths(sub_manifest)

filing_rows = []
parse_errors = []
for path, source_hash in latest_submissions:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            d = json.load(f)
        chunks = d.get("filings", {}).get("files", [])
        since_2010 = [
            row for row in parse_submission_payload(path, source_hash)
            if row["form_type"] in RELEVANT_FORMS and row["filing_date"] >= "2010-01-01"
        ]
        annual = [row for row in since_2010 if "10-K" in row["form_type"]]
        all_dates = [row["filing_date"] for row in since_2010]
        filing_rows.append({
            "cik":          d.get("cik", ""),
            "company":      d.get("name", ""),
            "annual_count": len(annual),
            "oldest":       min(all_dates, default=""),
            "has_chunks":   len(chunks) > 0,
        })
    except Exception as exc:
        parse_errors.append((path, exc))

if parse_errors:
    sample = "; ".join(f"{path}: {error}" for path, error in parse_errors[:5])
    raise RuntimeError(f"Coverage profiling failed for {len(parse_errors)} submissions payload(s): {sample}")

df = spark.createDataFrame(filing_rows)
total        = df.count()
chunks_count = df.filter(F.col("has_chunks")).count()
thresholds   = {
    f">={t}": f"{df.filter(F.col('annual_count') >= t).count()}/{total}"
    for t in [1, 5, 10, 14]
}

# COMMAND ----------

result = {
    "total_companies": total,
    "companies_with_older_chunk_files": chunks_count,
    "annual_filing_thresholds": thresholds,
    "sparse_companies": [r.asDict() for r in df.orderBy("annual_count").limit(10).collect()],
}

with open("/Volumes/sec_analytics/bronze/raw/stats_coverage.json", "w") as f:
    json.dump(result, f, indent=2)

dbutils.notebook.exit(json.dumps({"status": "ok", "companies_parsed": total}))
