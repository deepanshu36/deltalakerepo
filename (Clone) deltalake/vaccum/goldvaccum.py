# Databricks notebook source
# MAGIC %sql
# MAGIC create database gold

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled",False)

from delta.tables import DeltaTable

tables = spark.sql("SHOW TABLES IN gold").select("tableName").collect()


for table in tables:
    print(table.tableName)
    deltatable=DeltaTable.forName(spark,table.tableName)
    deltatable.vacuum(4)


   








