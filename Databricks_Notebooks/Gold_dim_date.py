# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create Flag Parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')


# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Creating Dimension Date

# COMMAND ----------

df_src = spark.sql('''SELECT DISTINCT(Date_ID) as Date_ID FROM PARQUET.`abfss://silver@carensdatalake.dfs.core.windows.net/Carsales`''')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### dim_sink as Intial and Incremental load

# COMMAND ----------

if spark.catalog.tableExists('cars_cataloge.gold.dim_date'):
    dim_sink = spark.sql('''
                     SELECT dim_date_id, Date_ID FROM cars_cataloge.gold.dim_date
                     ''')
else:
    dim_sink = spark.sql('''
                     SELECT 1 as dim_date_id, Date_ID FROM PARQUET.`abfss://silver@carensdatalake.dfs.core.windows.net/Carsales` WHERE 1=0
                     ''')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Filterning new records and old records

# COMMAND ----------

df_filter = df_src.join(dim_sink,df_src['Date_ID'] == dim_sink['Date_ID'], 'left').select(df_src['Date_ID'], dim_sink['dim_date_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Creating **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter['dim_date_id'].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Creating **df_filter_new**

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter['dim_date_id'].isNull()).select(df_filter['Date_ID'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating Surrogate Key
# MAGIC
# MAGIC #### **Fetch Max surrogate key from existing table**

# COMMAND ----------

if (incremental_flag == '0'):
    max_value = 1
else:
    df_max_value = spark.sql("SELECT Max(dim_date_id) FROM cars_catalog.gold.dim_date")
    max_value = df_max_value.collect()[0][0]+1

# COMMAND ----------


df_filter_new = df_filter_new.withColumn('dim_date_id',max_value+monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # SCD Type - I (Upsert)

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# incremental run
if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@carensdatalake.dfs.core.windows.net/dim_date")

    delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_date_id = src.dim_date_id')\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
                .execute()

# intital run
else:
    df_final.write.format("delta")\
        .mode("append")\
            .option("path", "abfss://gold@carensdatalake.dfs.core.windows.net/dim_date")\
                .saveAsTable("cars_catalog.gold.dim_date")


# COMMAND ----------

# MAGIC %sql
# MAGIC Select * FROM cars_catalog.gold.dim_date

# COMMAND ----------

