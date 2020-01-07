# BigDataProjects
Projects on Java, Scala,Pig,Hive

## Project 3: 
A directed graph is represented in the input text file using one line per graph vertex. 
For example, the line 1,2,3,4,5,6,7 represents the vertex with ID 1, which is linked to the vertices with IDs 2, 3, 4, 5, 6, and 7.

This Map-Reduce program Partitions a graph into K- Clusters using Multi source BFS. It selects K random graph vertices, called centroids, and then, at the first itearation, for each centroid, it assigns the centroid id to its unassigned neighbors. 
Then, at the second iteration. it assigns the centroid id to the unassigned neighbors of the neighbors, etc, in a breadth-first search fashion. After few repetitions, each vertex will be assigned to the centroid that needs the smallest number of hops to reach the vertex (the closest centroid). 

This program has 3 Map -Reduce jobs. The first Map-Reduce Job is to read the input and share vertex information with all nodes.
The second Map-reduce Job is repeated several number of times to make sure every node has been assigned a centroid. This program repeats the process for 8 times. 
The first Map-Reduce job writes on the directory args[1]+"/i0". The second Map-Reduce job reads from the directory args[1]+"/i"+i and writes in the directory args[1]+"/i"+(i+1), where i is the for-loop index you use to repeat the second Map-Reduce job. 
The final Map-Reduce job reads from args[1]+"/i8" and writes on args[2]. Note that the intermediate results between Map-Reduce jobs must be stored using SequenceFileOutputFormat.

## Project 4:
This project implements one step of the Lloyd's algorithm for k-means clustering in Scala. 
The goal is to partition a set of points into k clusters of neighboring points. It starts with an initial set of k centroids. Then, it repeatedly partitions the input according to which of these centroids is closest and then finds a new centroid for each partition. That is, if you have a set of points P and a set of k centroids C, the algorithm repeatedly applies the following steps: Assignment step: partition the set P into k clusters of points Pi, one for each centroid Ci, such that a point p belongs to Pi if it is closest to the centroid Ci among all centroids. Update step: Calculate the new centroid Ci from the cluster Pi so that the x,y coordinates of Ci is the mean x,y of all points in Pi.

To compile KMeans.scala, use command: 
**run kmeans.build**

To Run it in local mode:
**sbatch kmeans.local.run**

To Run it in Distributed mode: 
**sbatch kmeans.distr.run**


## Project 5: 
This project is Re-implementation of **Project 3** on Spark using Scala. We first construct the Vertex RDD similar to (id, Centroid, Adjacent).
Instead of Map-Reduce, we use Transformations such as flatmap, reduceByKey etc on the RDD constructed to clean the data and ultimately print the partition sizes.

## Project 6:
This project is Re-implementation of **Project 3** on Spark using PIG-Latin.

## Project 7: 
This project is Re-implementation of **Project 3** on Spark using HIVE.

## Project 8: 
This project is Re-implementation of **Project 5** using Pregel on Spark GraphX.

The following pseudo-code to do graph clustering using Pregel:

* Read the input graph and construct the RDD of edges
* Use the graph builder Graph.fromEdges to construct a Graph from the RDD of edges
* Access the VertexRDD and change the value of each vertex to be the -1 except for the first 5 nodes (these are the initial cluster number)
* Call the Graph.pregel method in the GraphX Pregel API to calculate the new cluster number for each vertex and propagate this number to the neighbors. For each vertex, this method changes its cluster number to the max cluster number of its neighbors only if the current cluster number is -1.
* Group the graph vertices by their cluster number and print the partition sizes 
