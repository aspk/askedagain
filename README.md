### Overview
Real-time community clustering analytics for Friendster's social network

### Details
Clustering, or community detection, is one of the most fundamental problems for social network analysis. Dynamic estimation of clusters in large social networks allows for real time insights into the current state and current direction of graph evolution that would not be available otherwise. 

Implement and verify latency/precision tests for [Hollocou et al](https://hal.archives-ouvertes.fr/hal-01639506v1/document)'s one pass streaming graph clustering algorithm

#### Model Assumptions
Nodes tend to be more connected (have more edges) within a community than across communities - thus, if edges are to arrive in random order, we would typically expect that intra-community edges would arrive before inter-community edges 

Edges will be inserted into stream in a random order

Assigns a threshold for the connectivity of a node - this is relatively determined, which seems a bit questionable

#### Streaming Model
Input Stream: sequence of edge insertion and edge deletion events

For every incoming edge, calculate degrees of vertices and estimate community of node according to a threshold

Update corresponding dictionaries for degree, community of node, node's community size 

##### Would like to also 
* Log changes for each cluster in a changelog - e.g. 

* Perform aggregation on the changelog provide analytics dashboard(cluster with most growth,largest cluster, smallest cluster) of cluster evolution over a sliding time window

### User Interface
Visualize real time cluster evolution


View analytics for each cluster
### Issues
Because low community size is the criteria for adding a node to that community, and the algorithm assumes that every node belongs to a community , outliers would be incorrectly grouped into a community

Also assumes community size is relatively even (because of the rule that chooses the community with the smaller member size for classifying an edge)

This algorithm would not work well on clustering large graphs with loosely defined communities - the algorithm relies on the assumption that there are significantly more intra-community nodes than inter-community nodes

Results are non-deterministic (shuffling the edge stream will generate different results as the algorithm is highly dependent on order of edge processing).🤨 However, this might(?) be ok if our goal is to generate a real-time approximation of clusters in the graph

* Verify precision of the one-pass algorithm by shuffling edges and observing community formation


## Pipeline
![Pipeline](https://raw.github.com/kellielu/friendster_communities/master/Pipeline.jpg)

## Dataset
#### Overview
Stanford's [Friendster network dataset](https://snap.stanford.edu/data/com-Friendster.html) where nodes represent users and edges mark friendship

Roughly 65 million nodes, 1.8 billion edges
	 
	