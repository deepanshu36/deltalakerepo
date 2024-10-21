# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists bronze.race
# MAGIC (
# MAGIC raceId integer,
# MAGIC year integer,
# MAGIC round integer,
# MAGIC circuitId integer,
# MAGIC name string,
# MAGIC date string,
# MAGIC time string,
# MAGIC url string,
# MAGIC fp1_date string,
# MAGIC fp1_time string,
# MAGIC fp2_date string,
# MAGIC fp2_time string,
# MAGIC fp3_date string,
# MAGIC fp3_time string,
# MAGIC quali_date string,
# MAGIC quali_time string,
# MAGIC sprint_date string,
# MAGIC sprint_time string,
# MAGIC source_location string,
# MAGIC load_time timestamp,
# MAGIC file_date string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (file_date)
# MAGIC location '/mnt/bronzeadls/race'
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.race
