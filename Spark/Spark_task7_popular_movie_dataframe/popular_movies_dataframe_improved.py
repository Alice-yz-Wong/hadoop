# Spark_Task7_popular_movie_dataframe_improved: find the top 10 movies
# join u.item and u.data,use movie name instead of movieID(use broadcast()) 

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

# load u.item for movie name
def loadMovieNames():
    movieNames = {}
    with codecs.open("hdfs:/ml-100k/u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Create spark session
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
# Create dictionary broadcast object, map movieID to movieName
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create schema 
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("hdfs:/ml-100k/u.data")

movieCounts = moviesDF.groupBy("movieID").count()

# Create a user-defined function to look up movie names from broadcasted dictionary
def lookupName(movieID):
    return nameDict.value[movieID]

lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column using udf
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# Sort results by count
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# Get top 10
sortedMoviesWithNames.show(10, False)

# Stop the session
spark.stop()
