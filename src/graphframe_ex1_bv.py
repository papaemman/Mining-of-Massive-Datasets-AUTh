from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import udf,lit,col,when
from pyspark import StorageLevel
from graphframes.examples import Graphs
from graphframes import *
from functools import reduce
import sys
import time
#from pyspark.sql import *







def main(TopK:str):

    sc = SparkSession.builder.appName("Top-k most probable triangles").getOrCreate()

       
    #load dataset(edge list) to dataframe 
    edgesDF = sc.read.option("header",True).option("inferSchema",True).csv("./ex.csv")

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
    edgesDF = edgesDF \
                    .select(edgeSrc(edgesDF.src,edgesDF.dst).alias("src"),edgeDst(edgesDF.src,edgesDF.dst).alias("dst"),"probability") 

    # create dataframe that consist the nodes
    nodesDF= edgesDF \
                .select(edgesDF.src) \
                .union(edgesDF.select(edgesDF.dst)) \
                .distinct() \
                .withColumnRenamed("src", "id") 


    # Create the Graph
    #g = GraphFrame(nodesDF,edgesDF)
    g = GraphFrame(nodesDF,edgesDF).cache()
    #g = GraphFrame(nodesDF,edgesDF).persist(StorageLevel.MEMORY_AND_DISK)

    #spaces = [0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,0]
    spaces = [0.7,0.4,0]
    #spaces = [0.5,0]



    trianglesFoundAlready = sc.sparkContext.broadcast(dict()) 


    # Finds all the triangles, "subgraph" = Dataframe
    for index,space in enumerate(spaces):
        
        newGraph = GraphFrame(nodesDF, g.edges.filter("probability > " + str(space)))
        #newGraph = g.filterEdges("probability > " + str(space))
        subgraph = newGraph.find("(a)-[e]->(b); (b)-[e2]->(c); (a)-[e3]->(c)")

        # Concatenate 3 strings
        @udf("string")
        def triangleName(node1:str, node2:str, node3:str)-> str:
            return node1 + "," + node2 + "," + node3


        """
        # Multiply 3 float
        @udf("float")
        def triangleProbCalc(edge1Prob, edge2Prob, edge3Prob)-> float:
            return float(edge1Prob) * float(edge2Prob) * float(edge3Prob)
`       """

        # calculates the triangle probability
        def triangleProbCalc(cnt:float, edge:str) -> float:
            probability = col(edge)["probability"]
            return cnt * probability


        schema = StructType([
                            StructField('Triangle', StringType(), True),
                            StructField('Triangle_Prob', FloatType(), True),
                            ])


        # creates new column ("Triangle) that contains the name of the triangle. Example, "143"
        # removes duplicates from the dataframe based on the "Triangle" column
        # creates new column ("Triangle_Prob") that contains the probability of the triangle
        # sorts the dataframe
        # Take the first k elements
        TopKTriangles = subgraph \
                            .withColumn("Triangle",triangleName(subgraph.a["id"],subgraph.b["id"],subgraph.c["id"])) \
                            .filter(col("Triangle").isin(list(trianglesFoundAlready.value.keys())) == False) \
                            .dropDuplicates(["Triangle"]) \
                            .withColumn("Triangle_Prob", reduce(triangleProbCalc, ["e", "e2", "e3"], lit(1))) \
                            .select("Triangle", "Triangle_Prob") \
                            .union(sc.createDataFrame([{"Triangle": triangleName, "Triangle_Prob":triangleProbability} for triangleName, triangleProbability in trianglesFoundAlready.value.items()], schema)) \
                            .sort("Triangle_Prob",ascending=False) \
                            .head(int(TopK)) 




        if len(TopKTriangles) == int(TopK):
            break
        else:
            trianglesFoundAlready = sc.sparkContext.broadcast(dict( [(row[0],row[1]) for row in TopKTriangles] ))
    
    for triangle in TopKTriangles:
        print(triangle)

    sc.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Give k as input")
        sys.exit()
    start = time.time()
    main(sys.argv[1])
    end = time.time()
    print("Execution time : " + str(end - start))
