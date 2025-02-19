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
# MAGIC # Creating Dimension Model

# COMMAND ----------

df_src = spark.sql('''SELECT DISTINCT(Model_ID) as Model_ID, model_category FROM PARQUET.`abfss://silver@carensdatalake.dfs.core.windows.net/Carsales`''')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### dim_sink as Intial and Incremental load

# COMMAND ----------

if spark.catalog.tableExists('cars_cataloge.gold.dim_model'):
    dim_sink = spark.sql('''
                     SELECT dim_model_id, Model_ID, model_category FROM cars_cataloge.gold.dim_model
                     ''')
else:
    dim_sink = spark.sql('''
                     SELECT 1 as dim_model_id, Model_ID, model_category FROM PARQUET.`abfss://silver@carensdatalake.dfs.core.windows.net/Carsales` WHERE 1=0
                     ''')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Filterning new records and old records

# COMMAND ----------

df_filter = df_src.join(dim_sink,df_src['Model_ID'] == dim_sink['Model_ID'], 'left').select(df_src['Model_ID'], df_src['model_category'], dim_sink['dim_model_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Creating **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter['dim_model_id'].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Creating **df_filter_new**

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter['dim_model_id'].isNull()).select(df_filter['Model_ID'], df_filter['model_category'])

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
    df_max_value = spark.sql("SELECT Max(dim_model_id) FROM cars_catalog.gold.dim_model")
    max_value = df_max_value.collect()[0][0]+1

# COMMAND ----------


df_filter_new = df_filter_new.withColumn('dim_model_id',max_value+monotonically_increasing_id())

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
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@carensdatalake.dfs.core.windows.net/dim_model")

    delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_model_id = src.dim_model_id')\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
                .execute()

# intital run
else:
    df_final.write.format("delta")\
        .mode("append")\
            .option("path", "abfss://gold@carensdatalake.dfs.core.windows.net/dim_model")\
                .saveAsTable("cars_catalog.gold.dim_model")


# COMMAND ----------

# MAGIC %sql
# MAGIC Select * FROM cars_catalog.gold.dim_model

# COMMAND ----------

