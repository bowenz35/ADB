# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Run RawtoBronze

# COMMAND ----------

# MAGIC %run ./rawtobronze


# COMMAND ----------

bronzeDF = spark.read.table("movie_bronze").filter("status = 'new'")


# COMMAND ----------

display(bronzeDF)

# COMMAND ----------


from pyspark.sql.types import *

schema = StructType([
    StructField("backdropUrl",StringType(),True),
    StructField("Budget",DoubleType(),True),
    StructField("CreatedBy",StringType(),True),
    StructField("CreatedDate",TimestampType(),True),
    StructField("Id",LongType(),True),
    StructField("ImdbUrl",StringType(),True),
    StructField("OriginalLanguage",StringType(),True),
    StructField("Overview",StringType(),True),
    StructField("PosterUrl",StringType(),True),
    StructField("Price",DoubleType(),True),
    StructField("ReleaseDate",TimestampType(),True),
    StructField("Revenue",DoubleType(),True),
    StructField("Runtime",LongType(),True),
    StructField("Tagline",StringType(),True),
    StructField("Title",StringType(),True),
    StructField("TmdbUrl",StringType(),True),
    StructField("UpdatedBy",StringType(),True),
    StructField("UpdatedDate",TimestampType(),True),
    StructField("genres",StructType([
        StructField("Id",IntegerType(),True),
        StructField("Name",StringType(),True),
    ]))
    
])


# COMMAND ----------

display(bronzeDF)

# COMMAND ----------

silverDF = bronzeDF.select("movie","movie.*")
silverDF = silverDF.dropDuplicates()


# COMMAND ----------

silverDF_quarantine = silverDF.where((silverDF.RunTime<0)|(silverDF.Budget<1000000))
silverDF_clean = silverDF.where((silverDF.RunTime>=0)&(silverDF.Budget>=1000000))

# COMMAND ----------

from delta.tables import DeltaTable
bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverQuarantine = silverDF_quarantine.withColumn(
    "status", lit("quarantined")
)

(
    bronzeTable.alias("bronze")
    .merge(silverQuarantine.alias("quarantine"), "bronze.movie = quarantine.movie")
    .whenMatchedUpdate(set={"status": "quarantine.status"})
    .execute()
)

# COMMAND ----------

from pyspark.sql.functions import *

langSilver = silverDF_clean.select("OriginalLanguage").distinct()
langSilver =langSilver.withColumn("id", monotonically_increasing_id())
display(langSilver)

# COMMAND ----------

silverPath = rootPath + "silver/"
silverLangPath =  silverPath + "lang/"

(
    langSilver.select(
        "id","OriginalLanguage"
    )
    .write.format("delta")
    .mode("append")
    .save(silverLangPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS language_silver
"""
)

spark.sql(
    f"""
CREATE TABLE language_silver
USING DELTA
LOCATION "{silverLangPath}"
"""
)

# COMMAND ----------

genreSilver=silverDF_clean.select(explode(silverDF_clean.genres).alias("genres"))
genreSilver=genreSilver.select("genres.id","genres.name").distinct()
genreSilver=genreSilver.where(genreSilver.name!="")
display(genreSilver.sort("id"))

# COMMAND ----------

silverGenrePath =  silverPath + "genre/"
(
    genreSilver.select(
        "id","name"
    )
    .write.format("delta")
    .mode("append")
    .save(silverGenrePath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS Genre_silver
"""
)

spark.sql(
    f"""
CREATE TABLE Genre_silver
USING DELTA
LOCATION "{silverGenrePath}"
"""
)


# COMMAND ----------

movieSilver=silverDF_clean.join(langSilver,langSilver.OriginalLanguage==silverDF_clean.OriginalLanguage,"left").select("BackdropUrl","Budget","CreatedBy","CreatedDate",silverDF_clean.Id,"ImdbUrl",langSilver.id.alias("LangID"),"Overview","PosterUrl","Price","ReleaseDate","Revenue","RunTime","Tagline","Title","TmdbUrl","UpdatedBy","UpdatedDate")

# COMMAND ----------

silverMoviePath =  silverPath + "movie/"
dbutils.fs.rm(silverMoviePath,recurse=True)

( movieSilver.select("BackdropUrl","Budget","CreatedBy","CreatedDate","Id","ImdbUrl","LangID","Overview","PosterUrl","Price","ReleaseDate","Revenue","RunTime","Tagline","Title","TmdbUrl","UpdatedBy","UpdatedDate"
    )
    .write.format("delta")
    .mode("append")
    .save(silverMoviePath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movie_silver
USING DELTA
LOCATION "{silverMoviePath}"
"""
)


# COMMAND ----------

genreMovieJunctionSilver=silverDF_clean.select("Id",explode(silverDF_clean.genres).alias("genres"))
genreMovieJunctionSilver=genreMovieJunctionSilver.select(col("Id").alias("MovieId"),col("genres.id").alias("GenreId"))
display(genreMovieJunctionSilver)

# COMMAND ----------

silverMovGenPath =  silverPath + "movie_genre_junction/"
(
    genreMovieJunctionSilver.select(
        "MovieId","GenreId"
    )
    .write.format("delta")
    .mode("append")
    .save(silverMovGenPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS Movie_Genre_silver
"""
)

spark.sql(
    f"""
CREATE TABLE Movie_Genre_silver
USING DELTA
LOCATION "{silverMovGenPath}"
"""
)


# COMMAND ----------

from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silverDF_clean.withColumn("status", lit("loaded"))

update_match = "bronze.movie = clean.movie"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)


# COMMAND ----------

qDF = spark.read.table("movie_bronze").filter("status = 'quarantined'")
display(qDF)

# COMMAND ----------

silverqDF = qDF.select("movie","movie.*")

movieSilver=silverqDF.join(langSilver,langSilver.OriginalLanguage==silverqDF.OriginalLanguage,"left").select("BackdropUrl","Budget","CreatedBy","CreatedDate",silverqDF.Id,"ImdbUrl",langSilver.id.alias("LangID"),"Overview","PosterUrl","Price","ReleaseDate","Revenue","RunTime","Tagline","Title","TmdbUrl","UpdatedBy","UpdatedDate")


movieSilverFix=movieSilver.withColumn("Budget",lit(1000000)).withColumn("RunTime",abs(col("RunTime")))


# COMMAND ----------

( movieSilver.select("BackdropUrl","Budget","CreatedBy","CreatedDate","Id","ImdbUrl","LangID","Overview","PosterUrl","Price","ReleaseDate","Revenue","RunTime","Tagline","Title","TmdbUrl","UpdatedBy","UpdatedDate"
    )
    .write.format("delta")
    .mode("append")
    .save(silverMoviePath)
)

# COMMAND ----------

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silverqDF.withColumn("status", lit("loaded"))

update_match = "bronze.movie = clean.movie"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)
