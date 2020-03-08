from pyspark import SparkConf, SparkContext
from math import radians, cos , sin , asin , sqrt


#Haversine formula to calculate distances
#we used Michael Dunn's implementation (https://stackoverflow.com/a/4913653/11136838)
def haversine(point1, point2):
    
    long1,lat1 = point1
    long2,lat2 = point2

    long1, lat1, long2, lat2 = map(radians, [float(long1), float(lat1), float(long2), float(lat2)])

    dlong = long2 - long1
    dlat = lat2 - lat1

    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2) * sin(dlong/2)**2
    c = 2 * asin(sqrt(a))
    R = 6371
    d = R * c
    return d



#Initializing coordinates for the centroids (k first points in our dataset)
def initialize_centroids(data, k):
    coords = data.take(k)

    centroids = {}
    for i in range(k):
        centroids[i] = coords[i]

    return centroids



#Finding the closest centroid to a point and assigning the corresponding label
def cluster(x, centroids):
    label = 0
    distance = float("inf")
    for item in centroids.items():
        new_dist = haversine(x,item[1])
        if new_dist < distance:
            label = item[0]
            distance = new_dist 

    return label
 


#Updating the coordinates of the centroids based on the points assigned to its cluster (mean value)
def update_centroids(data):
    data = data.map(lambda x : (x[1] , (x[0],1))) \
	.reduceByKey(lambda ((lon1,lat1),s1), ((lon2,lat2),s2): ((float(lon1)+float(lon2),float(lat1)+float(lat2)), s1+s2)) \
	.mapValues(lambda ((lon,lat),s): (lon/float(s), lat/float(s)))
	
    centroids = dict(data.collect())
    return centroids



#Main k-means algorithm
def k_means(k=5, MAX_ITERATIONS=3):
    centroids = initialize_centroids(data,k)
    iterations = 1

    while not (iterations > MAX_ITERATIONS):
        iterations = iterations + 1

        data_labeled = data.map(lambda x: (x,cluster(x,centroids)))
        new_centroids = update_centroids(data_labeled)
        centroids = new_centroids

    return centroids


if __name__ == "__main__":
    #Creating Spark Context
    conf = SparkConf().setAppName("k_means")
    sc = SparkContext(conf = conf)

    #Data input
    input_file = "hdfs://master:9000/yellow_tripdata_1m.csv"
    data = sc.textFile(input_file)

    #Data processing
    data = data.map(lambda line: (line.split(",")[3], line.split(",")[4]))

    #Removing false data (e.g. zero coordinates)  -- Optional 
    #For USA we assume 20<lat<50 and -130<lon<-60 (not including Alaska)
    data = data.filter(lambda atuple : float(atuple[0])>-130 and float(atuple[0])<-60 and float(atuple[1])>20 and float(atuple[1])<50)

    #Running k-means algorithm with default values (k=5, MAX_ITERATIONS=3)
    centroids = k_means()

    for (key,value) in centroids.items():
        print(str(key+1) + ":" + str(value))


