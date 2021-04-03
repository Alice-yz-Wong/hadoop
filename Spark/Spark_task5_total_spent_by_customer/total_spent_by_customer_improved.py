from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("Customerspent")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    itemID = int(fields[1])
    amountSpent=float(fields[2])
    return (customerID, amountSpent)

lines = sc.textFile("hdfs:///projects/customer-orders.csv") 
rdd = lines.map(parseLine)
#add amount spent together
totalSpent = rdd.reduceByKey(lambda x, y: x + y)
results=totalSpent.collect()

for result in results:
    print result

