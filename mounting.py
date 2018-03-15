# Databricks notebook source
#south east asia
spark.conf.set(
  "fs.azure.account.key.<account>.blob.core.windows.net",
  "<key>")

# COMMAND ----------

#east us
spark.conf.set(
  "fs.azure.account.key.<account>.blob.core.windows.net",
  "<key>")

# COMMAND ----------

spark.conf.set(
  "fs.azure.sas.spark.palaniblobeastus.blob.core.windows.net",
  "palaniblobeastus.blob.core.windows.net/spark/")

# COMMAND ----------

dbutils.fs.mount(source="wasbs://spark@palaniblobeastus.blob.core.windows.net/", mount_point="/mnt/s2/", extra_configs = {"fs.azure.account.key.<account>.blob.core.windows.net": "<key>"})

# COMMAND ----------

dbutils.fs.unmount('/mnt')

# COMMAND ----------

# MAGIC %fs ls /mnt/s2

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://sparkdataset@<account>.blob.core.windows.net/the-movies-dataset",
  mount_point = "/mnt/the-movies-dataset",
  extra_configs = {"fs.azure.account.key.blobpalani.blob.core.windows.net": "<key>"})

# COMMAND ----------

# MAGIC %fs ls /mnt/the-movies-dataset/

# COMMAND ----------

print sc
