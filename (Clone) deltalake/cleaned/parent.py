# Databricks notebook source


# COMMAND ----------

# MAGIC %run "/Workspace/Repos/deepanshuk36@gmail.com/deltalakerepo/(Clone) deltalake/importfunctions/mergecond"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/deepanshuk36@gmail.com/deltalakerepo/(Clone) deltalake/importfunctions/impfun"

# COMMAND ----------

dbutils.widgets.text("tablename","")
tablename=dbutils.widgets.get("tablename")

if(tablename in 'circuits'):
    key=circuitsKey
elif(tablename in 'drivers'):
    key=driversKey
elif(tablename in 'results'):
    key=resultsKey
elif(tablename in 'race'):
    key=raceKey
elif(tablename in 'laptimes'):
    key=laptimesKey           



# key=tablename+"Key"

# COMMAND ----------

print(key)

# COMMAND ----------

# %sql 
# delete from bronze.circuits
# where circuitId in (select circuitId from(
# select circuitId,count(*) from bronze.circuits
# group by 1
# having count(*)>1))


# COMMAND ----------

if(tablename=='all'):
    mergewithdelta(getlatest('/mnt/bronzeadls/{0}'.format(tablename)),'silver',tablename,circuitsKey)
    mergewithdelta(getlatest('/mnt/bronzeadls/{0}'.format(tablename)),'silver',tablename,driversKey)
    mergewithdelta(getlatest('/mnt/bronzeadls/{0}'.format(tablename)),'silver',tablename,resultsKey)
    mergewithdelta(getlatest('/mnt/bronzeadls/{0}'.format(tablename)),'silver',tablename,raceKey)
else:
    mergewithdelta(getlatest('/mnt/bronzeadls/{0}'.format(tablename)),'silver',tablename,key) 
       
