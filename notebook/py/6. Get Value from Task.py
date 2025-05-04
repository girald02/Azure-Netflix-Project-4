# Databricks notebook source
var = dbutils.jobs.taskValues.get(taskKey="fetchWeekDay", key="weekoutput", debugValue="default_value")

# COMMAND ----------

print(var)