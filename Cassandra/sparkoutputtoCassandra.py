from pyspark.sql import Sparksession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput(line):
    fields=line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]),gender=fields[2],occupation=fields[3],zip=fields[4])

if __name__="__main__":
    #create a sparksession
    spark=SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host","192.168.1.59").getOrCreate()

    #get the raw data
    lines=spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")

    #convert it to RDD of row object in format of (userID,age,gender,occupation,zip)
    users=lines.map(parseInput)

    #convert to dataframe
    usersDataset=spark.createDataFrame(users)

    #to cassandra
    usersDataset.write\
        .format("org.apache.sparl.sql.cassandra")\
        .mode('append')\
        .options(table="users",keyspace="movielens")\
        .save()
    
    #read from cassandra
    readUsers=spark.read\
    .format("org.apache.sparl.sql.cassandra")\
    .options(table="users",keyspace="movielens")\
    .load()

    readUsers.createOrReplaeTempView("users")

    sqlDF = spark.sql("SELECT*FROM users WHERE age<20")
    sqlDF.show()

    spark.stop()

