from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("hdfs:///projects/Book")

#take each line and split by white space
words = input.flatMap(lambda x: x.split())
#count how many time each value occur
wordCounts = words.countByValue()

#iteration, and encode
for word, count in wordCounts.items():
    #utfa, unicode, convert to ascii
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
