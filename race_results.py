# Databricks notebook source
race=spark.read.format('delta').load('abfss://processed@f1racersdata.dfs.core.windows.net/races')

# COMMAND ----------

cir=spark.read.format('delta').load('abfss://processed@f1racersdata.dfs.core.windows.net/circuits')

# COMMAND ----------

results=spark.read.format('delta').load('abfss://processed@f1racersdata.dfs.core.windows.net/results');

# COMMAND ----------

drivers=spark.read.format('delta').load('abfss://processed@f1racersdata.dfs.core.windows.net/drivers')

# COMMAND ----------

con=spark.read.format('delta').load('abfss://processed@f1racersdata.dfs.core.windows.net/constructors')

# COMMAND ----------

race.show(2)

# COMMAND ----------


cir.show(2);


# COMMAND ----------

results.show(2);


# COMMAND ----------

# MAGIC %md
# MAGIC # Joining the results and race and circuits and drivers to create the Dashboard data 

# COMMAND ----------

new_df=race.join(cir,race.circuit_id==cir.circuit_id, how='inner').select('race_id','race_year',race.name,cir.circuit_id,'location','date')

# COMMAND ----------

new_df.columns

# COMMAND ----------

data=new_df.join(results,new_df.race_id==results.race_id, how='inner')\
    .join(drivers,results.driver_id==drivers.driver_id, how='inner')\
        .join(con,results.constructor_id==con.constructor_Id, how='inner')

# COMMAND ----------

# MAGIC %md
# MAGIC # Fetch Only the Required Columns for the Dashboard data

# COMMAND ----------

dashboard_df=data.select(race.race_id,race.race_year,race.name.alias('race_name'),  race.date.alias('race_date'), cir.location.alias('circuit_location'), \
     drivers.name.alias('driver_name'), con.name.alias('constructor_name'), drivers.number.alias('driver_number'),drivers.nationality.alias('driver_nationality'), 'grid', 'fastest_lap',results.time.alias('race_time'), 'points')

# COMMAND ----------

# MAGIC %md
# MAGIC # Using persist to store the DashBoard_DataFrame 

# COMMAND ----------

from pyspark.storagelevel import StorageLevel


dashboard_df.persist(StorageLevel.MEMORY_AND_DISK);


# COMMAND ----------

# MAGIC %md
# MAGIC # Handling the null data

# COMMAND ----------

# MAGIC %md
# MAGIC  we can see null values in driver_number 
# MAGIC  
# MAGIC  so first we check the count then  we decide the approach
# MAGIC -  

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
display(dashboard_df.where(col('driver_number').isNull()).count())
display(dashboard_df.where(col('race_time').isNull()).count())   

# COMMAND ----------

# MAGIC %md
# MAGIC We can fill the null values instead of dropping the null values

# COMMAND ----------

dashboard_df=dashboard_df.fillna({'driver_number':0})
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import replace
dashboard_df=dashboard_df.withColumn('race_time', regexp_replace(col('race_time'),'N','00:00:00'))

# COMMAND ----------

#display(dashboard_df).limit(5)

# COMMAND ----------

dashboard_df=dashboard_df.select('driver_name','constructor_name','race_year','grid','fastest_lap','race_time','points') \
    .orderBy(col('points').desc())

# COMMAND ----------

display(dashboard_df);

# COMMAND ----------

dashboard_df.write.format('delta').option('path','abfss://presentation@f1racersdata.dfs.core.windows.net/race_results').saveAsTable('race_results')


# COMMAND ----------

