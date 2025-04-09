# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,current_timestamp, lit, to_timestamp, concat
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

r_schema=StructType(fields=[
                         
                         StructField("raceId", IntegerType(), nullable=False),
                         StructField("year", IntegerType(), True),
                         StructField("round", IntegerType(), True),
                         StructField("circuitId", IntegerType(), True),
                         StructField("name",StringType(),True),
                         StructField("date",DateType(),True),
                         StructField("time",StringType(),True),
                         StructField("url",StringType(),True),
                        
    
                         ])

# COMMAND ----------

data=spark.read.option("header",True).schema(r_schema).csv("abfss://raw@f1racersdata.dfs.core.windows.net/races.csv")

# COMMAND ----------

data.show(2)

# COMMAND ----------

data=data.withColumnRenamed("raceId","race_id");
data=data.withColumnRenamed("year","race_year");
data=data.withColumnRenamed("circuitId","circuit_id");

# COMMAND ----------

# MAGIC %md
# MAGIC # combine two columns date and time as race_timestamp
# MAGIC
# MAGIC data
# MAGIC
# MAGIC time
# MAGIC
# MAGIC Drop the url column
# MAGIC
# MAGIC insert the new column with currenttimestamp
# MAGIC

# COMMAND ----------

data=data.drop('url');

# COMMAND ----------

data=data.withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '), col('time')),"yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

final_df=data.withColumn("ingested_date",current_timestamp())


# COMMAND ----------

final_df.write.mode("append").format('delta').option('path','abfss://processed@f1racersdata.dfs.core.windows.net/races').saveAsTable('del_races');


# COMMAND ----------

