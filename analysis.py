# Databricks notebook source
# MAGIC %fs ls /mnt/the-movies-dataset/

# COMMAND ----------

movies = spark.read.csv('/mnt/the-movies-dataset/movies_metadata.csv', header=True, inferSchema=True)

# COMMAND ----------

movies.printSchema();

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType,DecimalType

# COMMAND ----------

# Note that we are removing all space characters from the col names to prevent errors when writing to Parquet later

moviesSchema = StructType([StructField('adult', StringType(), True),
                     StructField('belongs_to_collection', StringType(), True),
                     StructField('budget', DecimalType(), True),
                     StructField('genres', StringType(), True),                  
                     StructField('homepage', StringType(), True),       
                     StructField('id', IntegerType(), True),       
                     StructField('imdb_id', StringType(), True),       
                     StructField('original_language', StringType(), True),       
                     StructField('original_title', StringType(), True),       
                     StructField('overview', StringType(), True),       
                     StructField('popularity', DecimalType(), True),       
                     StructField('poster_path', StringType(), True),                  
                     StructField('production_companies', StringType(), True),       
                     StructField('production_countries', StringType(), True),       
                     StructField('release_date', StringType(), True),       
                     StructField('revenue', DecimalType(), True),       
                     StructField('runtime', DecimalType(), True),       
                     StructField('spoken_languages', StringType(), True),       
                     StructField('status', StringType(), True),                 
                     StructField('tagline', StringType(), True),       
                     StructField('title', StringType(), True),       
                     StructField('video', StringType(), True),       
                     StructField('vote_average', DecimalType(), True),       
                     StructField('vote_count', IntegerType(), True)])

# COMMAND ----------

movies = spark.read.csv('/mnt/the-movies-dataset/movies_metadata.csv', header=True, schema=moviesSchema);

# COMMAND ----------

movies.printSchema();

# COMMAND ----------

display(movies.limit(5))

# COMMAND ----------

#display(movies.filter('id==862'))
display(movies.filter('id==862'))

# COMMAND ----------

display(movies.filter('popularity > 20'));

# COMMAND ----------

print(movies.filter('popularity > 85').count());

# COMMAND ----------

movies.rdd.getNumPartitions()

# COMMAND ----------

movies.repartition(6).createOrReplaceTempView("MoviesView");

# COMMAND ----------

spark.catalog.cacheTable("MoviesView")

# COMMAND ----------

moviesTable = spark.table("MoviesView")

# COMMAND ----------

moviesTable.count()

# COMMAND ----------

display(moviesTable.limit(5))

# COMMAND ----------

print(moviesTable.filter('popularity > 90').count())

# COMMAND ----------

print('CSV count');
print(movies.count());
print('Table count');
print(moviesTable.count());

# COMMAND ----------


display(moviesTable.filter('popularity > 90').select('id').orderBy('id'))

# COMMAND ----------

display(movies.filter('popularity > 90').select('id').orderBy('id'))

# COMMAND ----------

print(moviesTable.filter('popularity > 90').count());
print(movies.filter('popularity > 90').count());


# COMMAND ----------

moviesTable.write.format('parquet').save('/mnt/the-movies-dataset/moviesParquet/')

# COMMAND ----------

# MAGIC %fs ls /mnt/the-movies-dataset/moviesParquet

# COMMAND ----------

moviesparquet = spark.read.parquet('/mnt/the-movies-dataset/moviesParquet')

# COMMAND ----------

moviesparquet.count()

# COMMAND ----------

display(moviesparquet.limit(2))

# COMMAND ----------

display(movies.limit(2))

# COMMAND ----------

# MAGIC %sql select count(*) from MoviesView;

# COMMAND ----------

# MAGIC %sql select count(*) from MoviesView where popularity > 90

# COMMAND ----------

# MAGIC %sql select status, count(status) as status_count from MoviesView group by status order by status_count desc limit 15

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",6)

# COMMAND ----------

# MAGIC %sql select count(*) from MoviesView where popularity > 90

# COMMAND ----------

spark.conf.get("spark.hadoop.hive.server2.use.SSL")

# COMMAND ----------

spark.conf.set("spark.hadoop.hive.server2.use.SSL","true")

# COMMAND ----------

# MAGIC %sql create table MoviesTable as select * from MoviesView

# COMMAND ----------

# MAGIC %sql select count(*) from moviestable

# COMMAND ----------

spark.catalog.cacheTable("MoviesTable")

# COMMAND ----------

# MAGIC %sql insert into MoviesTable select * from MoviesView

# COMMAND ----------

# MAGIC %sql create table MoviesTableNonCached as select * from MoviesTable

# COMMAND ----------

# MAGIC %sql select count(*) from MoviesTableNonCached

# COMMAND ----------

# MAGIC %sql select count(*) from MoviesTable

# COMMAND ----------

# MAGIC %sql select * from default.moviestablenoncached where id = 155 limit 1

# COMMAND ----------

# MAGIC %sql select * from default.moviestablenoncached where id = 155 limit 1

# COMMAND ----------

# MAGIC %sql create external table if not exists NewMovies (id string,age int) location '/mnt/newmoveis.csv';
