from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

#parse through the file, use , to divide the data, return(age,numFriends)
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

#create rdd
lines = sc.textFile("hdfs:///projects/fakefriends.csv")
#parse through each line and create rdd
rdd = lines.map(parseLine)
#map lambda function, number of friend export to tuple, map to function to count number of friend and number of ppl parsed
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#calculate average
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
