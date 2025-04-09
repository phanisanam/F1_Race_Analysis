# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,current_timestamp,lit,to_timestamp,concat
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

lap_schema=StructType(fields=[
                         
                         StructField("raceId", IntegerType(), nullable=False),
                        StructField("driverId", IntegerType(), nullable=True),
                        StructField("stop", StringType(), nullable=True),
                        StructField("duration", StringType(), nullable=True),
                         StructField("time", StringType(), True),
                         StructField("lap", IntegerType(), True),

])

# COMMAND ----------

data=spark.read.schema(lap_schema).csv("abfss://raw@f1racersdata.dfs.core.windows.net/lap_times/lap_times_split_1.csv");


# COMMAND ----------

data.count()