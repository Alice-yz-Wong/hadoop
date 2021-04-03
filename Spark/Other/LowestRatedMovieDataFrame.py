from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

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
    return Row(movieID=int(fields[1]),rating=float(fields[2]))

#main script
def __name__=="__main__":
    #create SparkSession
    spark=SparkSession.builder.appName("PopularMovies").getOrCreate() #getorcreate recover from failure

    #load movieID movie name lookup table
    movieNames=loadMovieNames()

    #load raw data
    line=sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    #convert to a RDD of Row objects with(movieID,rating)
    movies=line.map(parseInput)

    #convert to DataFrame
    movieDataset=spark.createDataFrame(movies)

    #calculate average rating
    averageRatings=movieDataset.groupBy("movieID").ave("rating")

    #join average and count 
    averagesAndCounts=counts.join(averageRatings,"movieID")

    #take top 10 result
    topTen = popularAveragesAndCounts.orderBy("avg(rating)").take(10)

    #print
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

        
    spark.stop()   



    
