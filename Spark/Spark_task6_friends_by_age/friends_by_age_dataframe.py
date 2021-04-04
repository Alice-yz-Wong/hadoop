# Spark_Task6_friends_by_age: User dataframe to find out the average 
# friend number by age

# Requirements:
# - sort by age
# - keep 2 decimal 
# - custom columns name

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

# use read to convert csv file to a dataframe, use hearder row for column name,inferSchema for data type
lines = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:/projects/fakefriends_header.csv")

# Extract only age and friends column
friendsByAge = lines.select("age", "friends")

# Group friends by age
friendsByAge.groupBy("age").avg("friends").show()

# Sort by age
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# Formatted average friends number to two decimal
friendsByAge.groupBy("age").agg(func.round(func.avg(float("friends")), 2)).sort("age").show()

# custom columns name to result
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)
  .alias("friends_avg")).sort("age").show()

spark.stop()
