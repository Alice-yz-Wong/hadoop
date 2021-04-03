from pyspark import SparkConf, SparkContext
#convert movieID to movie name
def loadMovieNames():
    movieNames = {}                                                                                                                                                     
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

#take each line and return(movieID,(rating,1.0))
def parseInput():
    fields=line.split()
    return (int(field[1]),(float(field[2]),1.0))

#main script
def __name__=="__main__":
    #create SparkContext
    conf=SparkConf().setAppName("worstmovies")
    sc=SparkContext(conf=conf)

    #load movieID movie name lookup table
    movieNames=loadMovieNames()

    #load raw data
    line=sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    #(movieID,(rating,1.0))
    movieRatings=line.map(parseInput)

    #reduce to(movieID,(sumOfRatings,totalRatings))
    ratingTotalAndCount=movieRatings.reduceByKey(lambda movie1, movie2:(movie1[0]+movie2[0],movie1[1]+movie2[1]))

    #map to(movieID, averageRating)
    averageRatings=ratingTotalAndCount.mapValues(lambda totalandCount:totalandCount[0]/ratingTotalAndCount[1])

    #sort by average rating
    sortedMovies=averageRatings.sortBy(lambda x:x[2])

    #take top 10 result
    results=sortedMovies(10)

    #print
    for result in results:
        print(movieNames[result[0]],result[1])
