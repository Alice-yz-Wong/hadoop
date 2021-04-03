from pyspark import SparkConf, SparkContext

# Create a dictionary for movie names
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# parse through each line of u.data and convert it to (movieID, (rating, 1.0))
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # Load movie ID -> movie name dictionary
    movieNames = loadMovieNames()

    # Load u.data file
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Convert to (movieID, (rating, 1.0))
    movieRatings = lines.map(parseInput)

    # Reduce to (movieID, (sumOfRatings, totalRatings))
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )

    # Filter out movies rated 10 or fewer times
    popularTotalsAndCount = ratingTotalsAndCount.filter(lambda x: x[1][1] > 10)

    # Map to (rating, averageRating)
    averageRatings = popularTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take top 10
    results = sortedMovies.take(10)

    # Print result
    for result in results:
        print(movieNames[result[0]], result[1])
