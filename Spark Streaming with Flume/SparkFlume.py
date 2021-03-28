import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

#extract data from apache log data
parts = [
    r'(?P<host>\S+)',                   # host
    r'\S+',                             # indent 
    r'(?P<user>\S+)',                   # user 
    r'\[(?P<time>.+)\]',                # time
    r'"(?P<request>.+)"',               # request
    r'(?P<status>[0-9]+)',              # status
    r'(?P<size>\S+)',                   # size 
    r'"(?P<referer>.*)"',               # referer
    r'"(?P<agent>.*)"',                 # user agent
]
pattern = re.compile(r'\s+'.join(parts)+r'\s*\Z')

#take line from raw file and extract the URL
def extractURLRequest(line):
    exp = pattern.match(line)
    if exp:
        request = exp.groupdict()["request"]
        if request:
           requestFields = request.split()
           if (len(requestFields) > 1):
                return requestFields[1]


if __name__ == "__main__":

    # setup sparkcomtext object, set log level, setup streaming context, interval of 1 sec 
    sc = SparkContext(appName="StreamingFlumeLogAggregator")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)
    
    #user flume util library. push model from flume to spark
    flumeStream = FlumeUtils.createStream(ssc, "192.168.1.59", 9092)

    #map operation
    lines = flumeStream.map(lambda x: x[1])
    urls = lines.map(extractURLRequest)

    # Reduce by URL over a 5-minute window sliding every second
    urlCounts = urls.map(lambda x: (x, 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y : x - y, 300, 1)

    # Sort and print the results
    sortedResults = urlCounts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))
    sortedResults.pprint()
    
    #create check point directory
    ssc.checkpoint("/home/maria_dev/checkpoint")
    
    #start and termination
    ssc.start()
    ssc.awaitTermination()
