# Databricks notebook source
# df_race=spark.read.table('silver.race').coalesce(1).cache()
# df_results=spark.read.table('silver.results').coalesce(1).cache()
# df_drivers=spark.read.table('silver.drivers').coalesce(1)



# COMMAND ----------

# from pyspark.sql.functions import *

# df_fact=df_results.join(broadcast(df_race),'raceId','inner').join(df_drivers,'driverId','inner').groupBy("raceId","driverId","resultId").agg(sum('points').alias('total_points'),count(when(col("position")==1,True)).alias('count'))
                                                                                                                                             
# df_fact.write.mode('overwrite').save('/mnt/goldadls/fact_table')
# df_fact.show(2)
# display(df_fact)






# COMMAND ----------

# from pyspark.sql.functions import *

# df_drivers=df_drivers.\
#     withColumn('fullname',concat(col("forename"),lit(" "),col("surname"))).\
#     withColumn('number',when(col("number")=='\\N',None).otherwise(col("number")).cast("int")).\
#     withColumn('code',when(col("number")=='\\N',None).otherwise(col("number")).cast("int")).drop('forename','surname') 


# display(df_drivers.limit(2))

# df_drivers.repartition(1).write.mode('overwrite').save('/mnt/goldadls/dim_drivers')





# COMMAND ----------

# # print(df_race.columns)
# from pyspark.sql.functions import *

# df_race=df_race.select([when(col(cn)=="\\N",None).otherwise(col(cn)).alias(cn) for cn in df_race.columns]).select(["raceId","year","round","circuitId","name"]) 

# display(df_race)

# df_race.repartition(1).write.mode('overwrite').save('/mnt/goldadls/dim_race')


# COMMAND ----------

from pyspark.sql.functions import *
df_circuits=spark.read.table('silver.circuits').cache()


df_circuits=df_circuits.select([when(col(cn)=="\\N",None).otherwise(col(cn)).alias(cn) for cn in df_circuits.columns]).select(["circuitId","circuitRef",	"name","location","country","lat","lng","alt"
]) 

# display(df_circuits)

df_circuits.repartition(1).write.mode('overwrite').save('/mnt/goldadls/dim_circuits')



# COMMAND ----------

# %sql 
# delete from silver.circuits
# where circuitId in (select circuitId from(
# select circuitId,count(*) from silver.circuits
# group by 1
# having count(*)>1))


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/mnt/goldadls/dim_circuits`
