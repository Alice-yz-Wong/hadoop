# Spark_task9_BFS_degree_of_separation: find out superhero social media
# return 9999 if not related, and return color as "white"
# show node to process as color "gray" 
# show node processed as color "black"
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

# The characters to find the degree of separation between:
startCharacterID = 5306 #SpiderMan
targetCharacterID = 14  #ADAM 3,031 

# BFS traversal, used to signal when target character was found
hitCounter = sc.accumulator(0)

# Convert graphs to BFS nodes, return key value pair in form of: (heroID,(connections,distance,color))
def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    # Create key value pair of start charactor: 
    #     - key-heroID  
    #     - value-(connections,distance,color)
    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0
    return (heroID, (connections, distance, color))

# Load in graph file
def createStartingRdd():
    inputFile = sc.textFile("hdfs:/project/Marvel+Graph")
    return inputFile.map(convertToBFS)

# Extract info from each node, look for gray node, change to "black" after processed
def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    # Look for gray node
    if (color == 'GRAY'):
        for connection in connections:
            #create new node
            newCharacterID = connection
            #increment the distance
            newDistance = distance + 1
            #color node gray
            newColor = 'GRAY'
            #when the target character is found, hitCounter increment by 1, 
            #signal to the driver script
            if (targetCharacterID == connection):
                hitCounter.add(1)
            #create node in form of (heroID,(connections,distance,color))
            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        #color it "black" to show that this node has been processed
        color = 'BLACK'

    #Emit the input node
    results.append( (characterID, (connections, distance, color)) )
    return results

# Gather all node generated, combine them by characterID, get the darkest color
def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)


#Main:
iterationRdd = createStartingRdd()
#set 10 as upper bound for connection
for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration+1))

    # Looking for gray node, signal when target character was found
    mapped = iterationRdd.flatMap(bfsMap)

    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)
