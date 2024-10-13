# Databricks notebook source
from pyspark.sql.functions import *


# COMMAND ----------

from pyspark.sql.functions import input_file_name,lit,split,current_timestamp

def addload_timestamp(df,path):
    if(path is None):
        return df.withColumn('source_location',split(input_file_name(),'/')[4]).withColumn('load_time',current_timestamp())
    else:
        print(path)
        return df.withColumn('source_location',lit(path)).withColumn('load_time',current_timestamp())


    

# COMMAND ----------

def checkschema(inputschema,sinkschema):
    if(inputschema==sinkschema):
        return True
    else:
        raise Exception("Schema Mismatch")



# COMMAND ----------

def getlatest(location):
    a=[]
    for path in dbutils.fs.ls(location):
        a.append((path[0],path[0].split('/')[-1][10:].split('.')[0].replace('_','-')))
    a.sort()
    return a[-1]

# COMMAND ----------

def mergewithdelta(path,db,dtname,matchcond):
    df_stage=spark.read.format('delta').load(path[0])

    # matchcond=mergecondition(df_stage)
    # df_stage=spark.read.format('delta').load(path[0])

    from delta.tables import DeltaTable
    deltatable=DeltaTable.forName(spark,f'{db}.{dtname}')
    deltatable.alias('dt').merge(df_stage.alias('st'),f'{matchcond}')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

# COMMAND ----------

# def mergecondition(df):
#     # 'dt.circuitId=st.circuitId'
#     import mack
#     pk=find_composite_key_candidates(df)
#     merge=""

#     for i in (0,len(pk)):
#         if (i==len(pk)-1):
#             merge+="dt.{pk[i]}=st.{pk[i]}"
#         else:
#             merge+="dt.{pk[i]}=st.{pk[i]} and "    

            













# COMMAND ----------

# try: 
#     import mack
# except ImportError:
#     %pip install mack

# COMMAND ----------


