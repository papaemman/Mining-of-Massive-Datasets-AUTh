from pyspark import SparkContext
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,lit,col,when
from graphframes.examples import Graphs
from graphframes import *
from functools import reduce
import os
import sys
import time
from csv import writer
#from pyspark.sql import *




def main(TopK:str, data_efficiency:str):

    sc = SparkSession.builder.appName("Top-k most probable triangles").getOrCreate()  
       
    # Load dataset(edge list) to dataframe 
    
    edgesDF = sc.read.option("header",True).option("inferSchema",True).csv("./input/ex_header.csv")
    
    # edgesDF = sc.read.option("header",False).option("inferSchema",True).csv("./input/collins.csv")
    # edgesDF = edgesDF.selectExpr("_c0 as src", "_c1 as dst", "_c2 as probability")

    # edgesDF.show() 
    # print(edgesDF.dtypes)


    # returns the smallest string between 2 strings
    @udf("string")
    def edgeSrc(src:str,dst:str)-> str:
        if src < dst: 
            return src
        return dst

    # returns the biggest string between 2 strings
    @udf("string")
    def edgeDst(src:str,dst:str)-> str:
        if src < dst: 
            return dst
        return src

    
    # re-order the source and destination of the edges. Source always the smallest id
    # edgesDF.show()
    edgesDF = edgesDF \
                    .select(edgeSrc(edgesDF.src,edgesDF.dst).alias("src"),edgeDst(edgesDF.src,edgesDF.dst).alias("dst"),"probability") 
    # edgesDF.show()

    # create dataframe that consist the nodes
    nodesDF= edgesDF \
                .select(edgesDF.src) \
                .union(edgesDF.select(edgesDF.dst)) \
                .distinct() \
                .withColumnRenamed("src", "id") 

    # Create the Graph
    if data_efficiency == "None":
        g = GraphFrame(nodesDF,edgesDF)
    elif data_efficiency == "cache":
        g = GraphFrame(nodesDF,edgesDF).cache()
    elif data_efficiency == "persist":
        g = GraphFrame(nodesDF,edgesDF).persist(StorageLevel.MEMORY_AND_DISK)


    # Finds all the triangles, "subgraph" = Dataframe
    subgraph = g.find("(a)-[e]->(b); (b)-[e2]->(c); (a)-[e3]->(c)")

    # Concatenate 3 strings
    @udf("string")
    def triangleName(node1:str, node2:str, node3:str)-> str:
        return node1 + "," + node2 + "," + node3

    # Na to kanw udf
    def triangleProbCalc(cnt, edge):
        edgeProbability = col(edge)["probability"]
        return cnt * edgeProbability

    




    # creates new column ("Triangle) that contains the name of the triangle. Example, "143"
    # removes duplicates from the dataframe based on the "Triangle" column
    # creates new column ("Triangle_Prob") that contains the probability of the triangle
    # sorts the dataframe
    # Take the first k elements
    
    
    # TopKTriangles = subgraph \
    #                         .withColumn("Triangle",triangleName(subgraph.a["id"],subgraph.b["id"],subgraph.c["id"])) \
    #                         .dropDuplicates(["Triangle"]) \
    #                         .withColumn("Triangle_Prob", reduce(triangleProbCalc, ["e", "e2", "e3"], lit(1))) \
    #                         .sort("Triangle_Prob",ascending=False) \
    #                         .select("Triangle","Triangle_Prob") \
    #                         .head(int(TopK)) 
    
    # .dropDuplicates(["Triangle"]) isn't necessary!
    
    TopKTriangles = subgraph \
                            .withColumn("Triangle",triangleName(subgraph.a["id"],subgraph.b["id"],subgraph.c["id"])) \
                            .withColumn("Triangle_Prob", reduce(triangleProbCalc, ["e", "e2", "e3"], lit(1))) \
                            .sort("Triangle_Prob",ascending=False) \
                            .select("Triangle","Triangle_Prob") \
                            .head(int(TopK)) 



    for triangle in TopKTriangles:
        print(triangle)

    sc.stop()
    

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Give k as input")
        sys.exit()
    start = time.time()
    main(TopK = sys.argv[1], data_efficiency = sys.argv[2])
    end = time.time()
    total_time = end-start
    print("Execution time : " + str(total_time))

    # Define a variable to store the number of cores (equivalent with spark executors in local mode)
    cores = 3

    with open('experiments.csv', 'a+', newline='') as experiments:
        # Create a writer object from csv module
        csv_writer = writer(experiments)
        # Add contents of list as last row in the csv file
        csv_writer.writerow([os.path.basename(__file__), cores, sys.argv[1], sys.argv[2], total_time])
        