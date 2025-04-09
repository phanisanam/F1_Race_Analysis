# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema=StructType(fields=[
                         
                         StructField("forename", StringType(), False),
                         StructField("surname", StringType(), True),
    
                         ])

# COMMAND ----------

d_schema=StructType(fields=[
                         
                         StructField("driverId", IntegerType(), nullable=False),
                         StructField("driverRef", StringType(), True),
                         StructField("code", StringType(), True),
                         StructField("dob", DateType(), True),
                         StructField("name",name_schema), #----> here i specify the schema directly
                         StructField("nationality",StringType(),True),
                         StructField("number",IntegerType(),True),
                         StructField("url",StringType(),True),
                        
    
                         ])

# COMMAND ----------

data=spark.read.option("header",True).schema(d_schema).json("abfss://raw@f1racersdata.dfs.core.windows.net/drivers.json")

# COMMAND ----------

data.show(3)

# COMMAND ----------

data=data.withColumnRenamed("driverId","driver_id")
data=data.withColumnRenamed("driverRef","driver_Ref");

# COMMAND ----------

data=data.drop('url');


# COMMAND ----------

data=data.withColumn("name",concat(col('name.forename'),lit(' '),col('name.surname')));


# COMMAND ----------

final_df=data.withColumn("ingested_date",current_timestamp())

# COMMAND ----------

final_df.write.mode('overwrite').format('delta').option('path','abfss://processed@f1racersdata.dfs.core.windows.net/drivers').saveAsTable('del_drivers')