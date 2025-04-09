# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,current_timestamp,lit,to_timestamp,concat
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import count, countDistinct, sum, avg

# COMMAND ----------

res_schema=StructType(fields=[
                         
                         StructField("constructorId", IntegerType(), nullable=False),
                        StructField("driverId", IntegerType(), nullable=False),
                        StructField("fastestLap", IntegerType(), nullable=False),
                        StructField("fastestLapSpeed", FloatType(), nullable=False),
                        StructField("fastestLapTime", StringType(), nullable=False),
                         StructField("grid", IntegerType(), True),
                         StructField("laps", IntegerType(), True),
                         StructField("milliseconds", IntegerType(), True),
                         StructField("number",IntegerType(),True),
                        StructField("points", FloatType(), nullable=False),
                        StructField("Position", IntegerType(), nullable=False),
                        StructField("positionOrder", IntegerType(), nullable=False),
                        StructField("positionText", StringType(), nullable=False),
                        StructField("raceId", IntegerType(), nullable=False),
                        StructField("rank", IntegerType(), nullable=False),
                        StructField("resultId", IntegerType(), nullable=False),
                        StructField("statusId", StringType(), nullable=False),
                        StructField("time", StringType(), nullable=False)
    
                         ])

# COMMAND ----------

data=spark.read.option("header",True).schema(res_schema) \
.json("abfss://raw@f1racersdata.dfs.core.windows.net/results.json")


# COMMAND ----------

data=data.withColumnRenamed("constructorId","constructor_id");
data=data.withColumnRenamed("driverId","driver_id");
data=data.withColumnRenamed("fastestLap","fastest_lap");
data=data.withColumnRenamed("fastestLapSpeed","fastest_lap_speed");
data=data.withColumnRenamed("fastestLapTime","fastest_lap_time");
data=data.withColumnRenamed("positionOrder","position_order");
data=data.withColumnRenamed("positionText","position_text");
data=data.withColumnRenamed("raceId","race_id");
data=data.withColumnRenamed("resultId","result_id");
data=data.withColumnRenamed("statusId","status_id");


# COMMAND ----------

data=data.drop(col('status_id'))

# COMMAND ----------

data=data.withColumn('ingested_date', current_timestamp())

# COMMAND ----------

data.write.mode("overwrite").format('delta').option('path','abfss://processed@f1racersdata.dfs.core.windows.net/results').saveAsTable('del_results')
