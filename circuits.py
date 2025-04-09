# Databricks notebook source
# MAGIC %run "../configs/paths"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

data=spark.read.option("header",True).csv(f"{raw_path}/circuits.csv")

# COMMAND ----------

data.show(3)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

cir_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), nullable=False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", FloatType(), True),
    StructField("long", FloatType(), True),
    StructField("alt", FloatType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

data=spark.read.option("header",True).schema(cir_schema).csv(f"{raw_path}/circuits.csv")

# COMMAND ----------

data.printSchema()

# COMMAND ----------

data=data.withColumnRenamed('circuitId','circuit_id')
data=data.withColumnRenamed('circuitRef','circuit_ref')   \
    .withColumnRenamed('lat','latitude') \
        .withColumnRenamed('long','longitude') \
            .withColumnRenamed('alt','altitude')

data.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping the columns

# COMMAND ----------

data = data.drop('url')
data.show(3)

# COMMAND ----------

data=data.withColumn("current_timestamp",current_timestamp())
data.show(2,truncate=False)

# COMMAND ----------

data.write.mode('overwrite').format('delta').option('path','abfss://processed@f1racersdata.dfs.core.windows.net/circuits').saveAsTable('del_circuits')

# COMMAND ----------



# COMMAND ----------

