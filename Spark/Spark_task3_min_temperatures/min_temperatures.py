#Spark_task3_min_temperatures: minimum temperature observed at different
# weather station in the year 1800

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = fields[3]
    return (stationID, entryType, temperature)

#create rdd
lines = sc.textFile("hdfs:///projects/1800.csv")
#parse through the file, out put (stationID, entryType, temperature)
parsedLines = lines.map(parseLine)
#filter out, only keep entryType with "TMIN"
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
#apply a new map(stationID,temperature)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
#find the mininum station temp
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
#collect result
results = minTemps.collect();
#iteration, 2 decimal place right of the decimal point
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
