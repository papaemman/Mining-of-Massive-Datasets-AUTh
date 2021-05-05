from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
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

    edgesDF.printSchema()
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
    #edgesDF = edgesDF.select(edgeSrc(edgesDF.src,edgesDF.dst).alias("src"),edgeDst(edgesDF.src,edgesDF.dst).alias("dst"),"probability") 
    #edgesDF = edgesDF.select(edgeSrc(edgesDF.src,edgesDF.dst).alias("src"),edgeDst(edgesDF.src,edgesDF.dst).alias("dst"),"probability").cache()
    edgesDF = edgesDF.select(edgeSrc(edgesDF.src,edgesDF.dst).alias("src"),edgeDst(edgesDF.src,edgesDF.dst).alias("dst"),"probability").persist( StorageLevel.MEMORY_AND_DISK)

    # create dataframe that consist the nodes
    nodesDF= edgesDF \
                .select(edgesDF.src) \
                .union(edgesDF.select(edgesDF.dst)) \
                .distinct() \
                .withColumnRenamed("src", "id") 

    #spaces = [0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1,0]
    spaces = [0.7,0.4,0]
    #spaces = [0.5,0]

    # Finds all the triangles, "subgraph" = Dataframe
    for index,space in enumerate(spaces):
        
        g = GraphFrame(nodesDF,edgesDF.filter(edgesDF.probability > space))
        subgraph = g.find("(a)-[e]->(b); (b)-[e2]->(c); (a)-[e3]->(c)")

        # Concatenate 3 strings
        @udf("string")
        def triangleName(node1:str, node2:str, node3:str)-> str:
            triangle = ""
            for node in sorted([str(node1),str(node2),str(node3)]):
                triangle += node 
            return triangle


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

        # creates new column ("Triangle) that contains the name of the triangle. Example, "143"
        # removes duplicates from the dataframe based on the "Triangle" column
        # creates new column ("Triangle_Prob") that contains the probability of the triangle
        # sorts the dataframe
        # Take the first k elements
        TopKTriangles = subgraph \
                            .withColumn("Triangle",triangleName(subgraph.a["id"],subgraph.b["id"],subgraph.c["id"])) \
                            .dropDuplicates(["Triangle"]) \
                            .withColumn("Triangle_Prob", reduce(triangleProbCalc, ["e", "e2", "e3"], lit(1))) \
                            .sort("Triangle_Prob",ascending=False) \
                            .select("Triangle", "Triangle_Prob") \
                            .take(int(TopK))


        #.withColumn("Triangle",triangleName(subgraph.a["id"],subgraph.b["id"],subgraph.c["id"])) \


        if len(TopKTriangles) == int(TopK):
            print(TopKTriangles)
            #print("Edw stamathse : " + str(index))
            break
    
    if len(TopKTriangles) < int(TopK): 
        print(TopKTriangles)
    sc.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Give k as input")
        sys.exit()
    start = time.time()
    main(sys.argv[1])
    end = time.time()
    print("Execution time : " + str(end - start))
