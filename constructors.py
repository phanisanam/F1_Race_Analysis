# Databricks notebook source
from pyspark.sql.functions import col,current_timestamp,lit,to_timestamp,concat
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

con_schema=StructType(fields=[
                         
                         StructField("constructorId", IntegerType(), nullable=False),
                         StructField("constructorRef", StringType(), True),
                         StructField("name", StringType(), True),
                         StructField("nationality", StringType(), True),
                         StructField("url",StringType(),True)
    
                         ])

# COMMAND ----------

data=spark.read.option("header",True).schema(con_schema) \
.json("abfss://raw@f1racersdata.dfs.core.windows.net/constructors.json")


# COMMAND ----------

data.show(3)

# COMMAND ----------

data=data.withColumnRenamed("constructorid","constructor_Id");
data=data.withColumnRenamed("constructorref","constructor_ref");


# COMMAND ----------

final_df=data.drop('url')

# COMMAND ----------

final_df=final_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

final_df.show()

# COMMAND ----------

final_df.write.mode('overwrite').format('delta').option('path','abfss://processed@f1racersdata.dfs.core.windows.net/constructors').saveAsTable('del_constructors')