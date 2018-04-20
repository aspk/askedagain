### Overview
Real-time community clustering analytics for Friendster's social network

### Details
Implement and verify latency/precision tests for [Hollocou et al](https://hal.archives-ouvertes.fr/hal-01639506v1/document)'s one pass streaming graph clustering algorithm

#### Model Assumptions
Nodes tend to be connected within a community than across communities - thus, if edges are to arrive in random order, we would expect intra-community edges to arrive before inter-community edges 

Edges will be inserted into stream in a random order

Assigns a threshold for the connectivity of a node - this is relatively determined, which seems a bit questionable

#### Streaming Model
Input Stream: sequence of edge insertion and edge deletion events

For every incoming edge, calculate degrees of vertices and estimate community of node according to a threshold

Update corresponding dictionaries for degree, community of node, node's community size 

Update metrics for cluster size, cluster relevance (connectivity percentile) for cluster metrics to SQL database


### Issues

This algorithm would not work well on clustering large graphs with loosely defined communities - the algorithm relies on the assumption that there are significantly more intra-community nodes than inter-community nodes

Results are non-deterministic (shuffling the edge stream will generate different results as the algorithm is highly dependent on order of edge processing).🤨 However, this might(?) be ok if our goal is to generate a real-time approximation of clusters in the graph

* Verify precision of the one-pass algorithm by shuffling edges and observing community formation


## Pipeline
![Pipeline](https://github.com/kellielu/friendster_communities)

## Dataset
#### Overview
Stanford's [Friendster network dataset](https://snap.stanford.edu/data/com-Friendster.html) where nodes represent users and edges mark friendship

Roughly 65 million nodes, 1.8 billion edges
	 
	