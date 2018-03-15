# Databricks notebook source
# MAGIC %fs ls '/mnt/s2/'

# COMMAND ----------

results = spark.read.csv('/mnt/s2/Results/Movies_Parquet_10Threads.csv', header=True, inferSchema=True)

# COMMAND ----------

results.printSchema();

# COMMAND ----------

results.show(5)

# COMMAND ----------

results = spark.read.csv('/mnt/s2/Results/Movies_Parquet_10Threads.csv', header=True, inferSchema=True)
count = results.count();
print(count);
count = int(count * 0.90);
print(count);
items = results.orderBy('TimeElapsed').limit(count);
items.agg({"TimeElapsed": "avg"}).show()

# COMMAND ----------

display(results)

# COMMAND ----------

results = spark.read.csv('/mnt/s2/Results/Movies_Parquet_singlethread.csv', header=True, inferSchema=True)
count = results.count();
print(count);
count = int(count * 0.95);
print(count);
items = results.orderBy('TimeElapsed').limit(count);
items.agg({"TimeElapsed": "avg"}).show()


# COMMAND ----------

display(results)

# COMMAND ----------

results = spark.read.csv('/mnt/s2/Results/MoviesCached_singlethread.csv', header=True, inferSchema=True)
count = results.count();
print(count);
count = int(count * 0.95);
print(count);
items = results.orderBy('TimeElapsed').limit(count);
items.agg({"TimeElapsed": "avg"}).show()

# COMMAND ----------

display(results)

# COMMAND ----------

#results = spark.read.csv('/mnt/s2/Results/MoviesCached_10Thread.csv', header=True, inferSchema=True)
results = spark.read.csv('/mnt/s2/Results/Movies_Cached_10Thread.csv', header=True, inferSchema=True)
count = results.count();
print(count);
count = int(count * 0.90);
print(count);
items = results.orderBy('TimeElapsed').limit(count);
items.agg({"TimeElapsed": "avg"}).show()

# COMMAND ----------

results = spark.read.csv('/mnt/s2/Results/Movies_CreditJoin_SingleThread.csv', header=True, inferSchema=True)
count = results.count();
print(count);
count = int(count * 0.95);
print(count);
items = results.orderBy('TimeElapsed').limit(count);
items.agg({"TimeElapsed": "avg"}).show()

# COMMAND ----------

results = spark.read.csv('/mnt/s2/Results/Movies_CreditJoin_Cached_SingleThread.csv', header=True, inferSchema=True)
count = results.count();
print(count);
count = int(count * 0.90);
print(count);
items = results.orderBy('TimeElapsed').limit(count);
items.agg({"TimeElapsed": "avg"}).show()

# COMMAND ----------

results = spark.read.csv('/mnt/s2/Results/sum_filter_10Threads_Non_Partitioned_1K.csv', header=True, inferSchema=True)
count = results.count();
print(count);
count = int(count * 0.95);
print(count);
items = results.orderBy('TimeElapsed').limit(count);
items.agg({"TimeElapsed": "avg"}).show()

# COMMAND ----------

display(results.orderBy('iteration'))

# COMMAND ----------

results = spark.read.csv('/mnt/s2/Results/ASI_10Threads_facet_status_filter_group_run2.csv', header=True, inferSchema=True)
count = results.count();
print(count);
count = int(count * 0.75);
print(count);
items = results.orderBy('TimeElapsed').limit(count);
items.agg({"TimeElapsed": "avg"}).show()

# COMMAND ----------

display(results)

# COMMAND ----------

resultswithru = spark.read.csv('/mnt/s2/Results/10IT_sum_filter_5Threads_Partitioned_50K.csv', header=True, inferSchema=True)
count = resultswithru.count();
print(count);
count = int(count * 0.99);
print(count);
items = resultswithru.orderBy('TimeElapsed').limit(count);
items.agg({"TimeElapsed": "avg"}).show()
items.agg({"RU": "avg"}).show();
itemsRU = resultswithru.orderBy('RU').limit(count);
resultswithru.agg({"RU": "avg"}).show();

# COMMAND ----------

display(resultswithru)

# COMMAND ----------

resultswithru = spark.read.csv('/mnt/s2/Results/10Threads_1000Iterations_Partitioned_Write.csv', header=True, inferSchema=True)
count = resultswithru.count();
print(count);
count = int(count * 0.95);
print(count);
items = resultswithru.orderBy('TimeElapsed').limit(count);
items.agg({"TimeElapsed": "avg"}).show()
items.agg({"RU": "avg"}).show();
itemsRU = resultswithru.orderBy('RU').limit(count);
resultswithru.agg({"RU": "avg"}).show();

# COMMAND ----------

display(resultswithru)
