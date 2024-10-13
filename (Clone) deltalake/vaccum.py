# Databricks notebook source
# MAGIC %sql use bronze 

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled",False)
from delta.tables import DeltaTable

df=spark.sql('show tables').select('tableName').collect()
for i in df:
    print (i["tableName"])
    deltatable=DeltaTable.forName(spark,i["tableName"])
    deltatable.vacuum(4)


