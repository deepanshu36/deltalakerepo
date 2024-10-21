# Databricks notebook source
# MAGIC %sql
# MAGIC create table bronze.results
# MAGIC (resultId long,
# MAGIC raceId long,
# MAGIC driverId long,
# MAGIC constructorId long,
# MAGIC number long,
# MAGIC grid long,
# MAGIC position long,
# MAGIC positionText string,
# MAGIC positionOrder long,
# MAGIC points double,
# MAGIC laps long,
# MAGIC time string,
# MAGIC milliseconds long,
# MAGIC fastestLap long,
# MAGIC rank long,
# MAGIC fastestLapTime string,
# MAGIC fastestLapSpeed double,
# MAGIC statusId long,
# MAGIC source_location string,
# MAGIC load_time timestamp,
# MAGIC file_date string)
# MAGIC using delta
# MAGIC partitioned by (file_date)
# MAGIC location '/mnt/bronzeadls/results'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.results
