import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("hdfs:///projects/Book")
words = input.flatMap(normalizeWords)
#break down map by value function: map the word to (x,1), and reduce by key and add value up for item with same key
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
#flip key and value(word, count) to value and key(count, word) for sorting
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
