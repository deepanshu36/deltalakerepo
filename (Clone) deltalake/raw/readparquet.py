# Databricks notebook source
# MAGIC
# MAGIC %run "/Workspace/Repos/deepanshuk36@gmail.com/deltalakerepo/(Clone) deltalake/importfunctions/impfun"

# COMMAND ----------

def getlatest(location):
    a=[]
    
    for path in dbutils.fs.ls(location):
        a.append((path[0],path[0].split('/')[-1][8:].split('.')[0].replace('_','-')))
    a.sort()
    if(len(a))==0:
        return None

    return a[-1]


filename=getlatest('/mnt/sourceeadlsgen2/results')[0]
filedate=getlatest('/mnt/sourceeadlsgen2/results')[1]

print(getlatest('/mnt/sourceeadlsgen2/results/')[0],filedate)

df=spark.read.format("parquet").option('header',True).option('inferschema',True).load(filename)

# display(df)

from pyspark.sql.types import *

# checkschema(df.dtypes,spark.read.table('bronze.results').dtypes[0:-3])

from pyspark.sql.functions import input_file_name,split,current_timestamp

df=addload_timestamp(df,None)
df=df.withColumn("file_date",lit(filedate))

# print(df.dtypes)
display(df)

df=df.dropDuplicates()

if(filedate in getlatest('/mnt/bronzeadls/results/')[0]):

    print(filedate,df["file_date"])
    df.write.partitionBy('file_date').mode('overwrite')\
    .option('replaceWhere', f"file_date=='{filedate}'")\
    .save('/mnt/bronzeadls/results/')

else:
    df.write.partitionBy('file_date').mode('append').save('/mnt/bronzeadls/results/')
