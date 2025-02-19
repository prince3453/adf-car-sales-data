# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Creating Fact table
# MAGIC
# MAGIC #### Reading silver data
# MAGIC

# COMMAND ----------

df_silver = spark.sql("SELECT * FROM PARQUET.`abfss://silver@carensdatalake.dfs.core.windows.net/Carsales`")



# COMMAND ----------

df_date = spark.sql("SELECT * FROM cars_catalog.gold.dim_date")
df_model = spark.sql("SELECT * FROM cars_catalog.gold.dim_model")
df_branch = spark.sql("SELECT * FROM cars_catalog.gold.dim_branch")
df_dealer = spark.sql("SELECT * FROM cars_catalog.gold.dim_dealer")

# COMMAND ----------

df_silver = df_silver.join(df_date, df_silver['Date_ID'] == df_date['Date_ID'],'left')\
                     .join(df_model, df_silver['Model_ID'] == df_model['Model_ID'],'left')\
                         .join(df_branch, df_silver['Branch_ID'] == df_branch['Branch_ID'],'left')\
                             .join(df_dealer, df_silver['Dealer_ID'] == df_dealer['Dealer_ID'],'left')\
                                 .select(df_silver['Revenue'], df_silver['RevenuePerUnit'], df_silver['Units_sold'], df_date['dim_date_id'], df_model['dim_model_id'], df_branch['dim_branch_id'], df_dealer['dim_dealer_id'])


# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Store data in Datalake

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("Facesales"):
    delta_tbl = DeltaTable.forPath(spark, "cars_catalog.gold.Factsales")

    delta_tbl.alias('trg').merge(df_silver.alias('src'), 'trg.dim_date_id = src.dim_date_id and trg.dim_model_id = src.dim_model_id and trg.dim_branch_id = src.dim_branch_id and trg.dim_dealer_id = src.dim_dealer_id')\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
                .execute()
else:
    df_silver.write.format("delta")\
        .mode("overwrite")\
            .option("path", "abfss://gold@carensdatalake.dfs.core.windows.net/Factsales")\
                .saveAsTable("cars_catalog.gold.Facesales")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM cars_catalog.gold.facesales;

# COMMAND ----------

