# Databricks notebook source
driverId integer
driverRef string
number string
code string
forename string
surname string
dob string
nationality string
url string

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/mnt/bronzeadls/drivers`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists bronze.drivers
# MAGIC (
# MAGIC driverId integer,
# MAGIC driverRef string,
# MAGIC number string,
# MAGIC code string,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC dob string,
# MAGIC nationality string,
# MAGIC url string,    
# MAGIC source_location string,
# MAGIC load_time timestamp, 
# MAGIC file_date string)
# MAGIC using delta
# MAGIC partitioned by (file_date)
# MAGIC location '/mnt/bronzeadls/drivers'
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.drivers
