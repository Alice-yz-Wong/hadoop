from starbase import Connection

c=Connection("192.168.1.59","4200")

#create a table call ratings
ratings=c.table('ratings')

#replace with the new one if already exist
if(ratings.exists()):
    print("Dropping existing ratings table\n")
    ratings.drop()

#create column family called rating
ratings.create('rating')

print("parsing the ml-100k ratings data...\n")
ratingFile=open("hdfs://sandbox.hortonworks.com:8020/root/tmp/maria_dev/ml-100k/u.data","r")

#batch process instead of one row
batch=ratings.batch()
for line in ratingFile:
    (userID,movieID,rating,time)=line.split()
    batch.update(userID,{'rating':{movieID:rating}})

ratingFile.close()

print("rating data into Hbase\n")
batch.commit(finalize=True)

#simulating print rating for user 1
print("get ratings for some user: ")
print("rating for user ID 1:\n")
print(ratings.fetch(1))

ratings.drop()
