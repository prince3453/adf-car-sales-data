# Databricks notebook source
# MAGIC %md
# MAGIC # Create catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE Catalog cars_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cars_catalog.gold.dim_model(
# MAGIC     dim_model_id INT,
# MAGIC     Model_ID STRING,
# MAGIC     model_category STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cars_catalog.gold.dim_dealer;
# MAGIC

# COMMAND ----------

