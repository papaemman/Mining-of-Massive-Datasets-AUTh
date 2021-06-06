from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf,lit,col,when
from pyspark import StorageLevel
from graphframes.examples import Graphs
from graphframes import *
from functools import reduce
import numpy as np
import sys
import time
#from pyspark.sql import *







def main(TopK:str,threshold:str):

    sc = SparkSession.builder.appName("Top-k most probable triangles").getOrCreate()

       
    #load dataset(edge list) to dataframe 
    edgesDF = sc.read.option("header",False) \
                        .option("inferSchema",True) \
                        .csv("./input/artists_power_law.csv") \
                        .selectExpr("_c0 as src", "_c1 as dst", "_c2 as probability")

    # artists_uniform.csv, artists_normal.csv, artists_power_law.csv

    # returns the smallest string between 2 integers
    @udf("integer")
    def edgeSrc(src:int,dst:int)-> int:
        if src < dst: 
            return src
        return dst

    # returns the biggest string between 2 integers
    @udf("integer")
    def edgeDst(src:int,dst:int)-> int:
        if src < dst: 
            return dst
        return src

    
    # re-order the source and destination of the edges. Source always the smallest id
    # Sort dataframe by Probability. Descending ordering
    edgesDF = edgesDF \
                    .select(edgeSrc(edgesDF.src,edgesDF.dst).alias("src"),edgeDst(edgesDF.src,edgesDF.dst).alias("dst"),"probability") \
                    .sort("probability", ascending=False) \
                    .cache()
    
    # create dataframe that consist the nodes
    nodesDF= edgesDF \
                .select(edgesDF.src) \
                .union(edgesDF.select(edgesDF.dst)) \
                .distinct() \
                .withColumnRenamed("src", "id") 

    try:
        # The 1st and 2nd edges with the highest probabilty in the graph
        FirstHeaviestEdge, SecondHeaviestEdge = edgesDF.head(2)
    except ValueError:
        print("The graph has not a triangle")
        sys.exit()

    # The biggest probabilty in the graph
    FirstHeaviestEdge_Probability  = float(FirstHeaviestEdge.probability)

    # The second biggest probabilty in the graph
    SecondHeaviestEdge_Probability  = float(SecondHeaviestEdge.probability)



    # Concatenate 3 strings
    @udf("string")
    def triangleName(node1:int, node2:int, node3:int)-> str:
        return str(node1) + "," + str(node2) + "," + str(node3)

    # calculates the triangle probability
    def triangleProbCalc(cnt:float, edge:str) -> float:
        probability = col(edge)["probability"]
        return cnt * probability



    # if heavy edges set do not have top-k triangles
    # then heavy edges has to be adjusted, namely, decrease the threshold
    for thd in np.arange(float(threshold), -0.1, -0.1):

        finalThreshold = thd 

        heavySubgraph = GraphFrame(nodesDF, edgesDF.filter(edgesDF["probability"] >= thd))
        trianglesInHeavySet = heavySubgraph.find("(a)-[e]->(b); (b)-[e2]->(c); (a)-[e3]->(c)")


        # creates new column "Triangle" that contains the name of the triangle. Example, "1,4,3"
        # creates new column "Triangle_Prob" that contains the probability of the triangle
        # sorts the dataframe in descending order
        # Take the first k elements
        TopKTrianglesInHeavySet = trianglesInHeavySet \
                            .withColumn("Triangle",triangleName(trianglesInHeavySet.a["id"],trianglesInHeavySet.b["id"],trianglesInHeavySet.c["id"])) \
                            .withColumn("Triangle_Prob", reduce(triangleProbCalc, ["e", "e2", "e3"], lit(1))) \
                            .sort("Triangle_Prob",ascending=False) \
                            .select("Triangle", "Triangle_Prob") \
                            .head(int(TopK)) 

        numberOfTriangles = len(TopKTrianglesInHeavySet)
        print("Number Of Triangles " + str(numberOfTriangles))

        if numberOfTriangles == int(TopK): # when heavy set has at least K triangles 

            # had to decrease the threshold to 0 to find the top-k triangles.
            # Consequently we are sure that the Top-k triangles of the heavy set are also the global Top-K 
            if finalThreshold == 0: 
                for triangle in TopKTrianglesInHeavySet:
                    print(triangle)
            break
    

    if numberOfTriangles == 0: # The graph has no triangles
        print("The graph has no triangles")
        sys.exit()

    elif numberOfTriangles < int(TopK): #The graph has less than K triangles
        for triangle in TopKTrianglesInHeavySet:
            print(triangle)

    elif finalThreshold != 0: #The Top-K triangles of the heavy set are maybe not the K global heaviest triangles

        lessHeaviestTriangleProbabilityInTheHeavySet = TopKTrianglesInHeavySet[-1].Triangle_Prob
        
        # The minimum probability that an edge must have
        # to maybe participate ina triangle that is heavier
        # from, at least, the less heaviest triangle found in the top-k heavies triangles in the heavy set
        minimumProbability = lessHeaviestTriangleProbabilityInTheHeavySet / (FirstHeaviestEdge_Probability * SecondHeaviestEdge_Probability)

        lightSet = edgesDF  \
                    .filter( edgesDF["probability"] < finalThreshold).filter(edgesDF["probability"]  >= minimumProbability) \
                    .sort(edgesDF["probability"],ascending=True)


        if lightSet.count() == 0: # The Top-K triangles of the heavy set are finally the top-K global heaviest triangles
            for triangle in TopKTrianglesInHeavySet:
                print(triangle)
        else: 

            # Edges that have probability < of threshold 
            # and probablity >= of the smallest probability that maybe
            # can form a triangle that has higher probability
            # from some Top-K triangle in the heavy set 
            graphThatContainsGlobalTopKTriangles = GraphFrame(nodesDF, edgesDF.filter(edgesDF["probability"] >= lightSet.head(1)[0].probability))

            triangles = graphThatContainsGlobalTopKTriangles.find("(a)-[e]->(b); (b)-[e2]->(c); (a)-[e3]->(c)")


            # creates new column "Triangle" that contains the name of the triangle. Example, "1,4,3"
            # creates new column "Triangle_Prob" that contains the probability of the triangle
            # sorts the dataframe in descending order
            # Take the first k elements
            GlobalTopKTriangles = triangles \
                            .withColumn("Triangle",triangleName(triangles.a["id"],triangles.b["id"],triangles.c["id"])) \
                            .withColumn("Triangle_Prob", reduce(triangleProbCalc, ["e", "e2", "e3"], lit(1))) \
                            .sort("Triangle_Prob",ascending=False) \
                            .select("Triangle", "Triangle_Prob") \
                            .head(int(TopK)) 

            for triangle in GlobalTopKTriangles:
                print(triangle)
    sc.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Give k as input")
        sys.exit()
    if len(sys.argv) < 3:
        print("Give Threshold as input")
        sys.exit()
    if len(sys.argv) == 3:
        if sys.argv[2] not in ["0.1","0.2","0.3","0.4","0.5","0.6","0.7","0.8","0.9"]:
            print("Threshold must be :")
            for i in ["0.1","0.2","0.3","0.4","0.5","0.6","0.7","0.8","0.9"]:
                print(i)
            sys.exit()
    start = time.time()
    main(sys.argv[1],sys.argv[2])
    end = time.time()
    print("Execution time : " + str(end - start))
