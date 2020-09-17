# PROJECT DESCRIPTION

An undirected graph is represented in the input text file using one line per graph vertex. <br />
For example, the line  <br />
1,2,3,4,5,6,7  <br />
represents the vertex with ID 1, which is connected to the vertices with IDs 2, 3, 4, 5, 6, and 7.

For example, the following graph:

![Graph](p2.png)
 <br />  <br />is represented in the input file as follows:
3,2,1 <br />
2,4,3 <br />
1,3,4,6 <br />
5,6 <br />
6,5,7,1 <br />
0,8,9 <br />
4,2,1 <br />
8,0 <br />
9,0 <br />
7,6 <br />


The Map-Reduce program finds <b> the connected components of any undirected graph and prints the size of these connected components</b>.  <br />A connected component of a graph is a subgraph of the graph in which there is a path from any two vertices in the subgraph.  <br />For the above graph, there are two connected components: one 0,8,9 and another 1,2,3,4,5,6,7. The program prints the sizes of these connected components: 3 and 7.


To compile and run the project navigate to the given directory and do the following steps: <br />
mvn install <br />
rm -rf output <br />
~/hadoop-2.6.5/bin/hadoop jar target/*.jar Graph small-graph.txt intermediate output


