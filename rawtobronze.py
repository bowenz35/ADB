# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Ingest Data

# COMMAND ----------

rootPath = "/Workspace/Shared/ADB/movie"

rawPath = rootPath + "raw/"
bronzePath = rootPath + "bronze/"


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS movie8")
spark.sql("USE movie8")

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

print(
    dbutils.fs.head(
        dbutils.fs.ls("dbfs:/movie/raw/")[0].path
    )
)


# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)


# COMMAND ----------

movie_data_df = (
    spark.read.option("multiline","true").json(files))
display(movie_data_df)

# COMMAND ----------

from pyspark.sql.functions import explode, col, to_json
movie_data_exploded_df = spark.read.json(path = f"/FileStore/tables/movie8/", multiLine = True)
movie_data_exploded_df = movie_raw.select("movie", explode("movie"))
movie_data_exploded_df = movie_raw.drop(col("movie")).toDF('movie')


# COMMAND ----------

movie_data_exploded_df = movie_data_exploded_df.dropDuplicates()
print(movie_data_exploded_df.count())
display(movie_data_exploded_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

movie_df = movie_data_exploded_df.select(
    "movie",
    lit("Manual Upload").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate"))


# COMMAND ----------

from pyspark.sql.functions import col
(movie_df.select(
        "datasource",
        "ingesttime",
        "movie",
        "status",
        col("ingestdate").alias("p_ingestdate"),
    )
.write.format("delta")
.mode("append")
.partitionBy("p_ingestdate")
.save(bronzePath))


# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_bronze
"""
)

spark.sql(
    f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
"""
)

