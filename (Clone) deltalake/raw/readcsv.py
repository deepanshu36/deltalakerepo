# Databricks notebook source
dbutils.widgets.text("filename","")
filename=dbutils.widgets.get("filename")
print(filename)


# COMMAND ----------

# MAGIC %run "/Workspace/Repos/deepanshuk36@gmail.com/deltalakerepo/(Clone) deltalake/importfunctions/impfun"

# COMMAND ----------


def getlatest(location):
    a=[]
    for path in dbutils.fs.ls(location):
        a.append((path[0],path[0].split('/')[-1][9:].split('.')[0].replace('_','-')))
    a.sort()
    return a[-1]

if(filename=='circuits' or filename=='all'):
    filename=getlatest('/mnt/sourceeadlsgen2/circuits')[0]
    filedate=getlatest('/mnt/sourceeadlsgen2/circuits')[1]
    df=spark.read.format("csv").option('header',True).option('inferschema',True).load(filename)
    from pyspark.sql.types import *
    checkschema(df.dtypes,spark.read.table('bronze.circuits').dtypes[0:-3])
    from pyspark.sql.functions import input_file_name,split,current_timestamp
    df=addload_timestamp(df,None)
    df=df.withColumn("file_date",lit(filedate))

    if(filedate in getlatest('/mnt/bronzeadls/circuits/')[0]):
        print(filedate,df["file_date"])
        df.write.partitionBy('file_date').mode('overwrite')\
        .option('replaceWhere', f"file_date=='{filedate}'")\
        .save('/mnt/bronzeadls/circuits/')

    else:
        df.write.partitionBy('file_date').mode('append').save('/mnt/bronzeadls/circuits/')




# COMMAND ----------


def getlatest(location):
    a=[]
    
    for path in dbutils.fs.ls(location):
        a.append((path[0],path[0].split('/')[-1][8:].split('.')[0].replace('_','-')))
    a.sort()
    if(len(a))==0:
        return None

    return a[-1]

if(filename=='drivers' or filename=='all'):
    filename=getlatest('/mnt/sourceeadlsgen2/drivers')[0]
    filedate=getlatest('/mnt/sourceeadlsgen2/drivers')[1]
    df=spark.read.format("csv").option('header',True).option('inferschema',True).load(filename)
    from pyspark.sql.types import *
    checkschema(df.dtypes,spark.read.table('bronze.drivers').dtypes[0:-3])
    from pyspark.sql.functions import input_file_name,split,current_timestamp
    df=addload_timestamp(df,None)
    df=df.withColumn("file_date",lit(filedate))

    if(filedate is not None and filedate in getlatest('/mnt/bronzeadls/drivers/')[0]):

        print(filedate,df["file_date"])
        df.write.partitionBy('file_date').mode('overwrite')\
        .option('replaceWhere', f"file_date=='{filedate}'")\
        .save('/mnt/bronzeadls/drivers/')

    else:
       df.write.partitionBy('file_date').mode('append').save('/mnt/bronzeadls/drivers/')


# COMMAND ----------


def getlatest(location):
    a=[]
    
    for path in dbutils.fs.ls(location):
        a.append((path[0],path[0].split('/')[-1][6:].split('.')[0].replace('_','-')))
    a.sort()
    if(len(a))==0:
        return None

    return a[-1]

if(filename=='race' or filename=='all'):
    filename=getlatest('/mnt/sourceeadlsgen2/race')[0]
    filedate=getlatest('/mnt/sourceeadlsgen2/race')[1]
    df=spark.read.format("csv").option('header',True).option('inferschema',True).load(filename)
    from pyspark.sql.types import *
    checkschema(df.dtypes,spark.read.table('bronze.race').dtypes[0:-3])
    from pyspark.sql.functions import input_file_name,split,current_timestamp
    df=addload_timestamp(df,None)
    df=df.withColumn("file_date",lit(filedate))
    if(filedate is not None and filedate in getlatest('/mnt/bronzeadls/race/')[0]):

        print(filedate,df["file_date"])
        df.write.partitionBy('file_date').mode('overwrite')\
        .option('replaceWhere', f"file_date=='{filedate}'")\
        .save('/mnt/bronzeadls/race/')

    else:
        df.write.partitionBy('file_date').mode('append').save('/mnt/bronzeadls/race/')


