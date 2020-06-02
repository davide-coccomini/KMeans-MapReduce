import sys
import math
import numpy as np
from operator import add
from pyspark import SparkContext

convergedCentroids = 0
centroids = []
new_centroids = []
iterations = 0
maxIterations = 100  

def calculateDistance(p1, p2):
    distance = 0
    for index in range(len(p1)):
        distance += (p1[index] - p2[index])**2
    
    return math.sqrt(distance)
            
def selectCentroid(point, centroids):
    bestIndex = 0
    closest = float("+inf")
    for centroid in centroids:
        tempDist = calculateDistance(point, centroid[1])
        if tempDist < closest:
            closest = tempDist
            bestIndex = centroid[0]
    return bestIndex


def partialSum(p1, p2):
    coordinates_sum = list( map(add, p1[0], p2[0]))
    points_number = p1[1] + p2[1]
    p = [coordinates_sum, points_number]
    return p


if __name__ == "__main__":
   
    if len(sys.argv) != 4:
        print("Usage: kmeans <k> <threshold> <file>", file=sys.stderr)
        sys.exit(-1)
    
    k = int(sys.argv[1])
    threshold = float(sys.argv[2])
    filename = sys.argv[3]
    
    master = "local"
    sc = SparkContext(master, "kmeans1")
    
    lines = sc.textFile(filename)
    
    ##CENTROIDS CONVERSION
    tmp = [line.split(",") for line in lines.takeSample(False, k)]
    
    for index, centroid in enumerate(tmp):
        centroids += [[index, [float(string) for string in centroid]]]
    
    
    ##POINTS CONVERSION
    points_rdd = lines.map(lambda line: [[float(string) for string in line.split(',')], 1])

    while(maxIterations > iterations):
        iterations += 1
        #MAP
        mapped_rdd = points_rdd.keyBy(lambda point : selectCentroid(point[0], centroids))

        #REDUCE
        reduced_rdd = mapped_rdd.reduceByKey(lambda p1, p2 : partialSum(p1, p2))

        reduced_points = reduced_rdd.collect()

        #print(reduced_points)

        new_centroids = []

        for index, reduced_point in enumerate(reduced_points):
            converted_point = list(reduced_point)
            centroid_index = converted_point[0]
            centroid_coordinates = np.array(converted_point[1][0])/converted_point[1][1]
            new_centroid = [centroid_index, centroid_coordinates]
            new_centroids.append(new_centroid)

        convergedCentroids = 0
        for index, centroid in enumerate(centroids):
            distance = calculateDistance(centroid[1], new_centroids[index][1])
            
            if distance < threshold:
                convergedCentroids+=1
        
        centroids = new_centroids
        
        percentage = len(centroids) * 80 / 100
        
        if convergedCentroids > percentage:
            print("Centroids converged")
            break
            
    
    print(new_centroids)
    sc.stop()
    
    
    
    
    
    
    