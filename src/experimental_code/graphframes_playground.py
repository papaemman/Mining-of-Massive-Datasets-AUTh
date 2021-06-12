## // Graohframes library //

# - Documentation: http://graphframes.github.io/graphframes/docs/_site/index.html
# - Run this script line-by-line using pyspark REPL.

# $ pyspark --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,lit,col,when
from graphframes import *
from graphframes.examples import Graphs


## // Setup //

# Define spark Session
sc = SparkSession.builder.appName("Top-k most probable triangles").getOrCreate()  


## // Load data //

# Load dataset(edge list) to dataframe 

edgesDF1 = sc.read.option("header",True).option("inferSchema",True).csv("./input/ex_header.csv")
edgesDF2 = sc.read.option("header",False).option("inferSchema",True).csv("./input/ex.csv")

edgesDF1.show()
edgesDF2.show()

type(edgesDF2)
edgesDF2 = edgesDF2.selectExpr("_c0 as src", "_c1 as dst", "_c2 as probability")


# Check data
edgesDF.show()
edgesDF.dtypes
edgesDF.head(5)


## // Define helper functions //

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
                .select(edgeSrc(edgesDF.src,edgesDF.dst).alias("src"), edgeDst(edgesDF.src,edgesDF.dst).alias("dst"),"probability") 
# edgesDF.show()

# create dataframe that consist the nodes
nodesDF= edgesDF \
            .select(edgesDF.src) \
            .union(edgesDF.select(edgesDF.dst)) \
            .distinct() \
            .withColumnRenamed("src", "id") 

nodesDF.show()

## // Graphframes //
g = GraphFrame(nodesDF,edgesDF)


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
                        .select("Triangle","Triangle_Prob") \
                        .head(int(TopK)) 

for triangle in TopKTriangles:
    print(triangle)

sc.stop()
