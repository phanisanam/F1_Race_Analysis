# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,current_timestamp,lit,to_timestamp,concat
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

p_schema=StructType(fields=[
                         
                         StructField("raceId", IntegerType(), nullable=False),
                        StructField("driverId", IntegerType(), nullable=True),
                        StructField("stop", StringType(), nullable=True),
                        StructField("duration", StringType(), nullable=True),
                         StructField("time", StringType(), True),
                         StructField("lap", IntegerType(), True),
                         StructField("milliseconds", IntegerType(), True)

])


# COMMAND ----------

# MAGIC %md
# MAGIC running Multiline Json file
# MAGIC

# COMMAND ----------

data=spark.read.schema(p_schema).option('multiline',True)\
.json("abfss://raw@f1racersdata.dfs.core.windows.net/pit_stops.json")

# COMMAND ----------

data=data.withColumnRenamed("driverId",'driver_id');
data=data.withColumnRenamed('raceId','race_id');
data=data.withColumn('ingested_date',current_timestamp())

# COMMAND ----------

data.write.mode('overwrite').format('delta').option('path','abfss://processed@f1racersdata.dfs.core.windows.net/pit_stops').saveAsTable('del_pit_stops');
