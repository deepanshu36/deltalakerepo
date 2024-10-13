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



# key=tablename+"Key"

# COMMAND ----------

print(key)

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from silver.circuits
# MAGIC where circuitId in (select circuitId from(
# MAGIC select circuitId,count(*) from silver.circuits
# MAGIC group by 1
# MAGIC having count(*)>1))
# MAGIC

# COMMAND ----------

if(tablename=='all'):
    mergewithdelta(getlatest('/mnt/bronzeadls/{0}'.format(tablename)),'silver',tablename,circuitsKey)
    mergewithdelta(getlatest('/mnt/bronzeadls/{0}'.format(tablename)),'silver',tablename,driversKey)
    mergewithdelta(getlatest('/mnt/bronzeadls/{0}'.format(tablename)),'silver',tablename,resultsKey)
    mergewithdelta(getlatest('/mnt/bronzeadls/{0}'.format(tablename)),'silver',tablename,raceKey)
else:
    mergewithdelta(getlatest('/mnt/bronzeadls/{0}'.format(tablename)),'silver',tablename,key) 
       
