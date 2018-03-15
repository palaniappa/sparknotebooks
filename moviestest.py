# Databricks notebook source
# MAGIC %sql desc MoviesTable

# COMMAND ----------

# MAGIC %sql create external table newmovies1 (adult string , belongs_to_collection string , budget decimal(10,0) , genres string , homepage string , id int , imdb_id string , original_language string , original_title string , overview string , popularity decimal(10,0) , poster_path string , production_companies string , production_countries string , release_date string , revenue decimal(10,0) , runtime decimal(10,0) , spoken_languages string , status string , tagline string , title string , video string , vote_average decimal(10,0) , vote_count int) location '/mnt/s2/newmovies1.csv';

# COMMAND ----------

# MAGIC %sql insert into newmovies1 select * from MoviesTable 

# COMMAND ----------

# MAGIC %sql select count(*) from NewMoviesDataSet limit 10

# COMMAND ----------



# COMMAND ----------

spark.catalog.cacheTable('NewMoviesDataSet');

# COMMAND ----------

# MAGIC %fs ls '/mnt/s2'

# COMMAND ----------

keywords = spark.read.csv('/mnt/s2/the-movies-dataset/keywords.csv');

# COMMAND ----------

keywords.show(5);

# COMMAND ----------

credits = spark.read.csv('/mnt/s2/the-movies-dataset/credits.csv',header=True,inferSchema=True);

# COMMAND ----------

credits.filter('id==24137').show()

# COMMAND ----------

credits.registerTempTable("CreditsTable");

# COMMAND ----------

credits.write.format('parquet').save('/mnt/s2/creditsParquet/');

# COMMAND ----------

creditsParquet = spark.read.parquet('/mnt/s2/creditsParquet/');

# COMMAND ----------

creditsParquet.registerTempTable("CreditsTable");

# COMMAND ----------

creditsParquet.write.saveAsTable("CreditsTable");

# COMMAND ----------

# MAGIC %sql select count(*) from CreditsTable

# COMMAND ----------

# MAGIC %sql select c.cast,c.crew,m.homepage,m.overview from newmovies1 m join CreditsTable c on m.id = c.id where m.id = 198466 limit 1

# COMMAND ----------

# MAGIC %sql select distinct c.id from newmovies1 m join CreditsTable c on m.id = c.id

# COMMAND ----------

# MAGIC %sql select * from NewMoviesDataSet m join CreditsTable c on m.id = c.id where m.id = 198466 limit 1

# COMMAND ----------

spark.catalog.cacheTable('CreditsTable');
