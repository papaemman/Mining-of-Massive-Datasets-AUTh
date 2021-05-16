from pyspark import SparkContext
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


def main(TopK:str):
    sc = SparkContext(appName="Top-k most probable triangles")
    
    # edgesRDD = sc.textFile("./input/ex.csv") 
    edgesRDD = sc.textFile("./input/collins.csv") 

    trianglesRDD = edgesRDD \
                    .map(lambda x: x.split(",")) \
                    .map(lambda x: reOrderingSrcAndDstOfEgde(x)) \
                    .groupByKey() \
                    .flatMap(lambda x: findEdgesToSearchToCalculateAllTriangles(x[0],list(x[1]))) \
                    .groupByKey() \
                    .flatMap(lambda x: returnTheSearchedEdgesThatExist(x[0], list(x[1]))) \
                    .groupByKey() \
                    .flatMap(lambda x: calculateTriangles(x[0], list(x[1]))) \
                    .sortBy(lambda x: x[1], ascending=False) \
                    .take(int(TopK)) 


    for triangle in trianglesRDD:
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
