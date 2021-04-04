# Spark_task1_ratings_counter: the objective of this task is to find out how 
# many of each rating occur

from pyspark import SparkConf, SparkContext
import collections #sort file function

#set master as local machine(in reality will be on cluster), set app name as "RatingsHistogram"
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#load data file from hdfs
lines = sc.textFile("hdfs:///ml-100k/u.data") #break up file line by line
ratings = lines.map(lambda x: x.split()[2]) #lambda, shortcut for passing function
result = ratings.countByValue()#count how many times each rating occur

#iteration
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
