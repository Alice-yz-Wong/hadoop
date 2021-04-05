from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit

#movie name dictionary
def loadMovieNames():
    movieNames={}
    with open("ml-100/u.item") as f:
        for line in f:
            fields=line.split('|')
            movieNames[int(fields[0]) = fields[1].decode('ascii','ignore')]
    return movieNames

#convert u.data line into (userID, movieID,rating) rows
def parseInput(line):
    fields=line.value.split()
    return Row(userID=int(fields[0]),movieID=int(fields[1]),rating=float(fields[2]))

if __name__=="__main__":
    #sparkSession
    spark=SparkSession.builder.appName("MovieRecs").getOrCreate()

    #load up movie ID to name dictionary
    line=spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    #convert it to a RDD of Row Object with(userID, movieID, Rating)
    ratingsRDD= lines.map(parseInput)

    #convert to a dataframe and cache it
    ratings=spark.createDataFrame(ratingsRDD).cache()

    #create an ALS collaborative filtering model from the complete data set
    #build model make prediction of data, train the model using data with known result, test model for dataset we don't know the result
    als=ALS(maxIter=5, regParam=0.01,userCol="userID",itemCol="movieID",ratingCol="rating", model=als.fit(ratings))

    # Print out ratings from user 0:
    print("\nRatings for user ID 0:")
    userRatings = ratings.filter("userID = 0")
    for rating in userRatings.collect():
        print movieNames[rating['movieID']], rating['rating']
                                                                                                                                                                        
    print("\nTop 20 recommendations:")
    # Find movies rated more than 100 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    # Construct a "test" dataframe for user 0 with every movie rated more than 100 times
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))
                                                                                                                                                                        
    # Run our model on that list of popular movies for user ID 0
    recommendations = model.transform(popularMovies)                                                                                                                    
                                                                                                                                                                        
    # Get the top 20 movies with the highest predicted rating for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)

    #iteration                                                                                                                                                                    
    for recommendation in topRecommendations:
        print (movieNames[recommendation['movieID']], recommendation['prediction'])
                                                                                                                                                                        
    spark.stop()

    







