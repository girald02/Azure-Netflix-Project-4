# Databricks notebook source
# MAGIC %md
# MAGIC ### Incremental loading using AUTOLOADER

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA netflix_catalog.net_schema;

# COMMAND ----------

# Create a dynamic variable
checkpoint_location = "abfss://silver@strgaccnetflixgirald.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation", checkpoint_location)\
  .load("abfss://raw@strgaccnetflixgirald.dfs.core.windows.net")

# COMMAND ----------

df.display()

# COMMAND ----------

df.writeStream \
  .option("checkpointLocation", checkpoint_location) \
  .trigger(processingTime='10 seconds') \
  .start("abfss://bronze@strgaccnetflixgirald.dfs.core.windows.net/netflix_titles")