## AskedAgain
Creating a distributed real-time duplicate question suggestion pipeline for Stack Overflow.

### Motivation
Duplicate questions on Stack Overflow:

* Deteriorate the overall quality of Stack Overflow knowledge base 

* Affect user experience 
	* Regular Stack Overflow users often find digging through several duplicate questions an unenjoyable and messy experience
	

Despite [recognizing this problem](https://stackoverflow.blog/2009/04/29/handling-duplicate-questions/), at the present, Stack Overflow still depends upon manual intervention from a small percentage of users - moderators and users with high reputation - to identify and mark duplicate questions. 

AskedAgain hopes to provide a streamlined, automatic pipeline to quickly identify potential duplicate questions to aid moderators and high reputation users in accelerating identification of duplicate questions. 
 

## Implementation Summary
### Incoming Stack Overflow Questions
* Questions are preprocessed in Spark: Question body and question are combinedtokenized, stop words removed, stemmed 
* Index incoming questions by tag (i.e. Javascript, Python)
* For each tag, use Min-Hashing and Locality Sensitive Hashing (Jaccard Similarity) to bucket similar questions
* Use an additional similarity metric + feedback from flagged questions to determine potential duplicates


### User Interface
* Allow duplicate questions to be sorted by tag
* Display mini graphs for potential duplicate questions, where nodes represent questions and edges connect potential duplicate quesitons. 
	* Edge weights represent the similarity between two questions, and the level of color saturation for a node represents the popularity(upvotes) of the question. 
	* We would like to identify the root question, or original question (such that all the other questions would then be considered duplicate questions) by the usefulness of the question, which seems to be best captured by popularity


### Engineering Challenges
* Implementing an online version of MinHash/LSH that is performant for a high thoroughput data stream
*  Ensure the Spark batch job will not fall too behind the high thoroughput input stream


### Architecture
![Architecture](https://raw.github.com/kellielu/askedagain/master/imgs/Architecture.jpg)
### Dataset
Stack Overflow data dump, available as a subset of the [Stack Exchange data dump](https://archive.org/details/stackexchange). 
The Stack Overflow dataset is also accessible on [Google Big Query](https://cloud.google.com/bigquery/public-data/stackoverflow).

### Visualization

## References
[1] [Deduplication in massive clinical notes dataset](https://arxiv.org/pdf/1704.05617.pdf)