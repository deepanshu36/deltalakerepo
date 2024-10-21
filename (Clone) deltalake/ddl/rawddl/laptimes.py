# Databricks notebook source
# MAGIC %sql
# MAGIC create table bronze.laptimes
# MAGIC (raceId integer,
# MAGIC driverId integer,
# MAGIC stop integer,
# MAGIC lap_time integer,
# MAGIC duration string,
# MAGIC milliseconds long,
# MAGIC source_location string,
# MAGIC load_time timestamp,
# MAGIC file_date string)
# MAGIC using delta
# MAGIC partitioned by (file_date)
# MAGIC location '/mnt/bronzeadls/laptimes'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.laptimes
