# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists silver

# COMMAND ----------

# MAGIC %sql drop table silver.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.circuits
# MAGIC (
# MAGIC circuitId int,
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
# MAGIC file_date string
# MAGIC )
# MAGIC using delta
# MAGIC location '/mnt/silveradls/circuits'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table silver.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.drivers
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
# MAGIC location '/mnt/silveradls/drivers'
# MAGIC

# COMMAND ----------

display(spark.read.table('silver.drivers')) 

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC create table if not exists silver.race
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
# MAGIC location '/mnt/silveradls/race'
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.results
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
# MAGIC location '/mnt/silveradls/results'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table silver.laptimes
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
# MAGIC location '/mnt/silveradls/laptimes'
