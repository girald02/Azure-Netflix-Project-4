# Databricks notebook source
dbutils.widgets.text("weekday", "7")

# COMMAND ----------

var_weekday = int(dbutils.widgets.get("weekday"));

# COMMAND ----------

dbutils.jobs.taskValues.set(key="weekoutput" , value= var_weekday);