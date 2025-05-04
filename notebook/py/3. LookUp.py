# Databricks notebook source
# MAGIC %md
# MAGIC ### ARRAY PARAMETER

# COMMAND ----------

files = [
    {
        "source_folder" : "netflix_directors",
        "target_folder" : "netflix_directors"
    },
    {
        "source_folder" : "netflix_countries",
        "target_folder" : "netflix_countries"
    },
    {
        "source_folder" : "netflix_cast",
        "target_folder" : "netflix_cast"
    },
    {
        "source_folder" : "netflix_category",
        "target_folder" : "netflix_category"
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC **JOB UTILITY TO RETURN THE [ARRAY]**

# COMMAND ----------

dbutils.jobs.taskValues.set(key="my_arr", value=files)