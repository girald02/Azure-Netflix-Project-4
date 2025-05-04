# Databricks notebook source
# MAGIC %md
# MAGIC ## # Silver Note Book LookUp Table

# COMMAND ----------

# MAGIC %md
# MAGIC **PARAMETERS**

# COMMAND ----------

dbutils.widgets.text("source_folder", "netflix_directors")
dbutils.widgets.text("target_folder", "netflix_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC **VARIABLES**

# COMMAND ----------

var_src_folder = dbutils.widgets.get("source_folder");
var_target_folder = dbutils.widgets.get("target_folder")

# COMMAND ----------

# PATH
bronze_path_netflix_directors = "abfss://bronze@strgaccnetflixgirald.dfs.core.windows.net/netflix_directors";
silver_path_netflix_directors = "abfss://silver@strgaccnetflixgirald.dfs.core.windows.net/netflix_directors";

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@strgaccnetflixgirald.dfs.core.windows.net/{var_src_folder}")
df.display()

# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
        .option("path", f"abfss://silver@strgaccnetflixgirald.dfs.core.windows.net/{var_target_folder}")\
        .save()