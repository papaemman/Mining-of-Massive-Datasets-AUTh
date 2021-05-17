from pyspark import SparkContext
import numpy as np
import sys
import time


# Put node with smaller id as src of edge and node with bigger id as dst.  
def reOrderingSrcAndDstOfEgde(x:list)-> tuple:
    src = x[0]
    dst = x[1]
    probability = x[2]
    if src < dst:
        return (src,(dst,probability))
    else:
        return (dst,(src,probability))


# Find which edges must exist to create triangles with bases a specific node
# Returns also all the already existig edges.
def findEdgesToSearchToCalculateAllTriangles(node:str, listOfEdges:list)-> list:
    
    listOfEdges.sort(key= lambda x: x[0])
    edgesToSearchAndEdgesThatExist = list()
   
    for index,edge in enumerate(listOfEdges):
        dstNode = edge[0]
        edge_probability = edge[1]
        edgesToSearchAndEdgesThatExist.append( ((node,dstNode), (edge_probability,"-1")) ) # The edges that exist. The "-1" is a flag that shows that this edge exist 
        for edge2 in listOfEdges[index + 1:]: # Calculate all the edges that are need to create triangles with basis this node
            # The value "X" is dummy and is need in order to have the same structure in all key-value pairs
            # The node in the key-value pair shows which node needs this edge to create a triangle
            edgesToSearchAndEdgesThatExist.append( ((dstNode,edge2[0]), ("X",node)) )  
    return edgesToSearchAndEdgesThatExist


# return the edges that were searched to calculate the existing triangles to the nodes that need them 
# returns also the edges to the node from which they were extracted 
def returnTheSearchedEdgesThatExist(edge:tuple, listThatShowsIfEdgeExistAndShowsNodesThatNeedEdgeToCreateTriangles:list)-> tuple:

    listThatShowsIfEdgeExistAndShowsNodesThatNeedEdgeToCreateTriangles.sort(key= lambda x: x[1]) # put fisrt element of the list the key-value pair that shows if edge exist
    edgesToReturn = list()

    if listThatShowsIfEdgeExistAndShowsNodesThatNeedEdgeToCreateTriangles[0][1] == "-1": # Edge exist in the graph
        edgeProbability = listThatShowsIfEdgeExistAndShowsNodesThatNeedEdgeToCreateTriangles[0][0] 
        edgesToReturn.append( (edge[0],(edge,edgeProbability)) )   
        for keyValuePairThatShowsWhichNodeNeedTheEdgeToCreateTriangle in listThatShowsIfEdgeExistAndShowsNodesThatNeedEdgeToCreateTriangles[1:]:
            edgesToReturn.append( (keyValuePairThatShowsWhichNodeNeedTheEdgeToCreateTriangle[1], (edge,edgeProbability)) )
        return edgesToReturn   
    else:  # Edge doesn't exist in the graph
        return edgesToReturn # edgesToReturn = []




# Finds the triangles that exist with basis a node and their probabilities
def calculateTriangles(node, listOfEdges:list)-> list:
    listOfEdges.sort(reverse=True,key= lambda x: x[0][0])
    edges_dic = dict((key,value) for key, value in listOfEdges) 
    trianglesThatExist = list()
    for edge1 in listOfEdges:
        if edge1[0][0] > node: # triangle exist with bases this node
            edge2 = (node,edge1[0][0])
            edge3 = (node,edge1[0][1])
            triangleName = node + "," + edge1[0][0] + "," + edge1[0][1]
            triangleProbability = float(edges_dic[edge1[0]]) * float(edges_dic[edge2]) * float(edges_dic[edge3])
            trianglesThatExist.append((triangleName,triangleProbability))
        else: # no triangle exist with bases this node 
            break
    return trianglesThatExist


def main(TopK:str, threshold:str):

    sc = SparkContext(appName="Top-k most probable triangles")
    

    data = sc.textFile("./input/collins.csv")
    
    # data = sc.textFile("./input/artists_uniform.csv")
    data = sc.textFile("./input/artists_normal.csv")
    # data = sc.textFile("./input/artists_power_law.csv")

    preprocessedEdges_RDD = data \
                            .map(lambda x: x.split(",")) \
                            .sortBy(lambda x: x[2], ascending=False) \
                            .map(lambda x: reOrderingSrcAndDstOfEgde(x)) 

    # preprocessedEdges_RDD = sc.textFile("./input/ex.csv") \
    #                         .map(lambda x: x.split(",")) \
    #                         .sortBy(lambda x: x[2], ascending=False) \
    #                         .map(lambda x: reOrderingSrcAndDstOfEgde(x)) 

    heavyTriangles = list()
    finalThreshold = threshold
    # if heavy edges set do not have top-k triangles
    # then heavy edges has to change
    for thd in np.arange(float(threshold), 0, -0.1):

        finalThreshold = str(thd)

        # Edges that have probability >= of threshold 
        # The Heavy edges set
        heavyEdges_RDD = preprocessedEdges_RDD.filter(lambda x: x[1][1] >= str(thd)).cache() 


        if heavyEdges_RDD.count() >= 3: # There is a possibility to have a triangle in the heavy set
            # The 1st and 2nd edges with the highest probabilty in the graph
            FirstHeaviestEdge, SecondHeaviestEdge = heavyEdges_RDD.take(2)

            # The biggest probabilty in the graph
            FirstHeaviestEdge_Probability  = float(FirstHeaviestEdge[1][1])

            # The second biggest probabilty in the graph
            SecondHeaviestEdge_Probability  = float(SecondHeaviestEdge[1][1])
        else:
            continue

    
        # Store in a list the top-k triangles with their probabilities
        # We are not yet sure that the Top-K triangles that we find
        # in the Heavy edges set are the global Top-K heavy triangles
        heavyTriangles = heavyEdges_RDD \
                                    .groupByKey() \
                                    .flatMap(lambda x: findEdgesToSearchToCalculateAllTriangles(x[0],list(x[1]))) \
                                    .groupByKey() \
                                    .flatMap(lambda x: returnTheSearchedEdgesThatExist(x[0], list(x[1]))) \
                                    .groupByKey() \
                                    .flatMap(lambda x: calculateTriangles(x[0], list(x[1]))) \
                                    .sortBy(lambda x: x[1], ascending=True) \
                                    .take(int(TopK)) 
        
        if len(heavyTriangles) == int(TopK):  # Heavy edges set has top-k triangles
            if finalThreshold == "0":
                for i in heavyTriangles:
                    print(i)
            break

    if not heavyTriangles: # The graph has no triangles
        print("The graph has no triangles")
        sys.exit()

    if len(heavyTriangles) < int(TopK): # Global top-k have found already
        for i in heavyTriangles:
            print(i)

    elif finalThreshold != "0": 

        lessHeaviestTriangleProbabilityInTheHeavySet = heavyTriangles[-1][1]

        minimumProbability = lessHeaviestTriangleProbabilityInTheHeavySet / (FirstHeaviestEdge_Probability * SecondHeaviestEdge_Probability)

    

        # Edges that have probability < of threshold 
        # and probablity >= of the smallest probability that maybe
        # can form a triangle that has higher probability
        # from some Top-K triangle in the heavy set 
        lightEdges_RDD = preprocessedEdges_RDD \
                                        .filter(lambda x: x[1][1] < finalThreshold) \
                                        .filter(lambda x: float(x[1][1]) >= minimumProbability)

    
        finalHeavyEdges = heavyEdges_RDD \
                                        .union(lightEdges_RDD) \
                                        .groupByKey() \
                                        .flatMap(lambda x: findEdgesToSearchToCalculateAllTriangles(x[0],list(x[1]))) \
                                        .groupByKey() \
                                        .flatMap(lambda x: returnTheSearchedEdgesThatExist(x[0], list(x[1]))) \
                                        .groupByKey() \
                                        .flatMap(lambda x: calculateTriangles(x[0], list(x[1]))) \
                                        .sortBy(lambda x: x[1], ascending=False) \
                                        .take(int(TopK)) 

    
    
        for i in finalHeavyEdges:
            print(i)

    sc.stop()


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Give k as input")
        sys.exit()
    
    if len(sys.argv) < 3:
        # print("Give Threshold as input")
        # sys.exit()
        k = sys.argv[1]
        threshold = 0.8

    if len(sys.argv) == 3:
        if sys.argv[2] not in ["0.1","0.2","0.3","0.4","0.5","0.6","0.7","0.8","0.9"]:
            print("Threshold must be :")
            for i in ["0.1","0.2","0.3","0.4","0.5","0.6","0.7","0.8","0.9"]:
                print(i)
            sys.exit()
        k = sys.argv[1]
        threshold = sys.argv[2]
    
    start = time.time()
    main(k,threshold)
    end = time.time()
    print("Execution time : " + str(end - start))
