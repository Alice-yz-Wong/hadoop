# Spark_task8_most_popular_superhero_dataframe: find the superhero that show up the most with 
# other superheros in comic books

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create spark Session
spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()
# Create schema
schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

# Parse through marvel name, separate by space
names = spark.read.schema(schema).option("sep", " ").csv("hdfs:/projects/Marvel+Names")
# Read file Marvel-graph
lines = spark.read.text("hdfs:/projects/Marvel+Graph")

# Extract id, then count how many columns for each id(subtract first entry), group by id
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
# Sort by connections in descending order
mostPopular = connections.sort(func.col("connections").desc()).first()
# Get name of the superhero, use id to select the name
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()
# Print report
print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

