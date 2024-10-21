# Databricks notebook source
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled",False)

from delta.tables import DeltaTable

tables = spark.sql("SHOW TABLES IN silver").select("tableName").collect()

display(tables)

for table in tables:
    print(table.tableName)
    deltatable=DeltaTable.forName(spark,table.tableName)
    deltatable.vacuum(4)


   








