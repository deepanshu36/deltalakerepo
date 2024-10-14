# Databricks notebook source
# MAGIC %run "/Workspace/Repos/deepanshuk36@gmail.com/deltalakerepo/(Clone) deltalake/importfunctions/impfun"

# COMMAND ----------

def getlatest(location):
    a=[]
    
    for path in dbutils.fs.ls(location):
        a.append((path[0],path[0].split('/')[-1][10:].split('.')[0].replace('_','-')))
    a.sort()
    if(len(a))==0:
        return None

    return a[-1]

from pyspark.sql.types import *

schema=StructType([StructField('raceId',IntegerType()),
                  StructField('driverId',IntegerType()),
                   StructField('stop',IntegerType()), 
                   StructField('lap_time',IntegerType()),
                   StructField('duration',StringType()),
                   StructField('milliseconds',LongType())])

filename=getlatest('/mnt/sourceeadlsgen2/laptimes')[0]
filedate=getlatest('/mnt/sourceeadlsgen2/laptimes')[1]

print(getlatest('/mnt/sourceeadlsgen2/laptimes/')[0],filedate)

df=spark.read.format('csv').schema(schema).option('header',True).load(filename)


# display(df)

from pyspark.sql.types import *

checkschema(df.dtypes,spark.read.table('bronze.laptimes').dtypes[0:-3])

from pyspark.sql.functions import input_file_name,split,current_timestamp

df=addload_timestamp(df,None)

df=df.withColumn("file_date",lit(filedate))

# print(df.dtypes)
#
if(filedate is not None and filedate in getlatest('/mnt/bronzeadls/laptimes/')[0]):
    print(filedate,df["file_date"])
    df.write.partitionBy('file_date').mode('overwrite')\
    .option('replaceWhere', f"file_date=='{filedate}'")\
    .save('/mnt/bronzeadls/laptimes/')

else:
    df.write.partitionBy('file_date').mode('append').save('/mnt/bronzeadls/laptimes/')
