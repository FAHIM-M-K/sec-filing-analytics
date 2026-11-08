#  Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS sec_analytics;
# MAGIC CREATE SCHEMA IF NOT EXISTS sec_analytics.bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS sec_analytics.bronze.raw;

# COMMAND ----------