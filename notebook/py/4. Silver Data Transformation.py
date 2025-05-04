# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver Data Transformation

# COMMAND ----------

#Import Essentials Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
      .option("header", "true")\
      .option("inferschema", "true")\
      .load("abfss://bronze@strgaccnetflixgirald.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

# Fill for "NULL" values 
df = df.fillna({'duration_minutes': 0, 'duration_seasons': 1})

# COMMAND ----------

df = df.withColumn("duration_minutes", df.duration_minutes.cast(IntegerType()))\
        .withColumn("duration_seasons", df.duration_seasons.cast(IntegerType()))

# COMMAND ----------

df = df.withColumn("Short_Title", split(df.title, ":").getItem(0))
df.display()

# COMMAND ----------

df = df.withColumn("rating", split(df.rating, "-").getItem(0))
df.display()

# COMMAND ----------

df = df.withColumn("type_flag" , when(df.type == "Movie", 1)\
                                .when(df.type == "TV Show", 2)\
                                .otherwise(0))
df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn("duration_ranking", dense_rank().over(Window.orderBy(df.duration_minutes.desc())))

# COMMAND ----------

df.display()

# COMMAND ----------

# Save df to sql within the notebook only
df.createOrReplaceTempView("mydataframe")

# COMMAND ----------

# Save df to sql within GLOBAL notebooks
df.createOrReplaceGlobalTempView("myglobaldataframe")

# COMMAND ----------

# Load df from sql
df = spark.sql("SELECT * FROM global_temp.myglobaldataframe")

# COMMAND ----------

df.display()

# COMMAND ----------

# Data Aggregate
df_visual = df.groupBy("type").agg(count("*").alias("total_count"))

# COMMAND ----------

target_path_netflix_titles = "abfss://silver@strgaccnetflixgirald.dfs.core.windows.net/netflix_titles";

# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
        .option("path" , target_path_netflix_titles)\
        .save()