# Spark_Task7_popular_movie_dataframe: find the top 10 movies

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create schema
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load u.data into a dataframe, separate by tab, use schema, read csv file from hdfs
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("hdfs:/ml-100k/u.data")

# group by movieID, count then order by count in decending order,
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# show top 10
topMovieIDs.show(10)

# Stop the session
spark.stop()
