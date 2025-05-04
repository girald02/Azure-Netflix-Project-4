# Databricks notebook source
# MAGIC %md
# MAGIC ### DLT - Gold Layer

# COMMAND ----------

looktables_rules = {
    "rule_1" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "gold_netflix_directors"
)
@dlt.expect_or_drop("rule_1", "show_id is NOT NULL")
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@strgaccnetflixgirald.dfs.core.windows.net/netflix_directors")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflix_cast"
)
@dlt.expect_or_drop("rule_1", "show_id is NOT NULL")
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@strgaccnetflixgirald.dfs.core.windows.net/netflix_cast")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflix_category"
)
@dlt.expect_or_drop("rule_1", "show_id is NOT NULL")
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@strgaccnetflixgirald.dfs.core.windows.net/netflix_category")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflix_countries"
)
@dlt.expect_or_drop("rule_1", "show_id is NOT NULL")
def myfunc():
    df = spark.readStream.format("delta").load("abfss://silver@strgaccnetflixgirald.dfs.core.windows.net/netflix_countries")
    return df

# COMMAND ----------

@dlt.table

def gold_stg_netflix_titles():

    df = spark.readStream.format("delta").load("abfss://silver@strgaccnetflixgirald.dfs.core.windows.net/netflix_titles")
    return df 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

@dlt.view

def gold_trns_netflix_titles():
    df = spark.readStream.table("LIVE.gold_stg_netflix_titles")
    df = df.withColumn("newFlag" , lit(1))
    return df

# COMMAND ----------

master_data_rules = {
    "rule1": "newFlag IS NOT NULL",
    "rule2": "show_id IS NOT NULL"
}

# COMMAND ----------

@dlt.table

@dlt.expect_all_or_drop(master_data_rules)
def gold_netflix_titles():
    df = spark.readStream.table("LIVE.gold_trns_netflix_titles")
    df = df.withColumn("newFlag" , lit(1))
    return df