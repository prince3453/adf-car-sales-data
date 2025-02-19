# Databricks notebook source
# MAGIC %md
# MAGIC # Read data

# COMMAND ----------

df = spark.read.format('parquet')\
            .option('inferschema', True)\
            .load('abfss://bronze@carensdatalake.dfs.core.windows.net/rawdata')

# COMMAND ----------

df.display()

# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.withColumn('model_category', split(col('Model_ID'),'-')[0])
df.display()

# COMMAND ----------

df.withColumn('Units_sold',col('Units_sold').cast(StringType())).display()

# COMMAND ----------

df = df.withColumn('RevenuePerUnit', col('Revenue')/col('Units_sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Ad-hoc Analysis

# COMMAND ----------

df.groupBy('YEAR','BranchName').agg(sum('Units_sold').alias('Total_Units')).sort('YEAR','Total_Units', ascending=[1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Saving Data to Silver Layer of Madellion architecture

# COMMAND ----------

df.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silver@carensdatalake.dfs.core.windows.net/Carsales')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Quering Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PARQUET.`abfss://silver@carensdatalake.dfs.core.windows.net/Carsales`

# COMMAND ----------

