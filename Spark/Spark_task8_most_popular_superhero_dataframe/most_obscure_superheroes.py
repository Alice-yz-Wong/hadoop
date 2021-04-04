# Spark_task8_most_popular_superhero_dataframe: find the superhero that show up least 
# with other superheros in comic books

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create spark session
spark = SparkSession.builder.appName("MostObscureSuperheroes").getOrCreate()

# Create schema
schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])
# Read name file
names = spark.read.schema(schema).option("sep", " ").csv("hdfs:/projects/Marvel+Names")
# Read comic book data
lines = spark.read.text("hdfs:/projects/Marvel+Graph")

# Extract id, then count how many columns for each id(subtract first entry), group by id
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Find the mininum connection     
minConnectionCount = connections.agg(func.min("connections")).first()[0]

# Filter item that have the mininum connections
minConnections = connections.filter(func.col("connections") == minConnectionCount)

# Join operation of all superhero with one connection
minConnectionsWithNames = minConnections.join(names, "id")

# Print
print("The following characters have only " + str(minConnectionCount) + " connection(s):")

minConnectionsWithNames.select("name").show()