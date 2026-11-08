#  Databricks notebook source

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS sec_analytics;
# MAGIC CREATE SCHEMA IF NOT EXISTS sec_analytics.bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS sec_analytics.bronze.raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;
# MAGIC SHOW SCHEMAS IN sec_analytics;
# MAGIC SHOW VOLUMES IN sec_analytics.bronze;