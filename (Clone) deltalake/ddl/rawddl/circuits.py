# Databricks notebook source
# MAGIC %sql
# MAGIC create database bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists bronze.circuits
# MAGIC (circuitId int, 
# MAGIC circuitRef string, 
# MAGIC name string, 
# MAGIC location string,
# MAGIC country string, 
# MAGIC lat double,
# MAGIC lng double, 
# MAGIC alt string, 
# MAGIC url string, 
# MAGIC source_location string,
# MAGIC load_time timestamp, 
# MAGIC file_date string)
# MAGIC using delta
# MAGIC partitioned by (file_date)
# MAGIC location '/mnt/bronzeadls/circuits'
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.circuits
