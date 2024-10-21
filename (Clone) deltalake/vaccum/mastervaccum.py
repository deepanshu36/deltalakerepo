# Databricks notebook source
# parameteries
%run /Workspace/Repos/deepanshuk36@gmail.com/deltalakerepo/(Clone) deltalake/vaccum/bronzevaccum


# COMMAND ----------

dbutils.widgets.text("databasename","")
databasename=dbutils.widgets.get("databasename")



if(databasename in 'bronze'):
    %run /Workspace/Repos/deepanshuk36@gmail.com/deltalakerepo/(Clone) deltalake/vaccum/bronzevaccum
elif(databasename in 'silver'):
    %run /Workspace/Repos/deepanshuk36@gmail.com/deltalakerepo/(Clone) deltalake/vaccum/silvervaccum
elif (databasename in 'gold'):
    %run /Workspace/Repos/deepanshuk36@gmail.com/deltalakerepo/(Clone) deltalake/vaccum/goldvaccum
else:
    raise Exception("invalid databse name")    
    




