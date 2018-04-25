## AskedAgain
Creating a distributed real-time duplicate question suggestion pipeline for Stack Overflow.

### Motivation
Duplicate questions on Stack Overflow present a conundrum for users looking to efficiently find answers to questions that may have already been answered. 

Despite [recognizing this problem](https://stackoverflow.blog/2009/04/29/handling-duplicate-questions/), at the present, Stack Overflow still depends upon manual intervention from a small percentage of users - moderators and users with high reputation - to identify and mark duplicate questions. 

AskedAgain hopes to provide a streamlined, automatic pipeline to quickly identify potential duplicate questions to aid moderators and high reputation users in accelerating identification of duplicate questions. 
 

## Implementation Summary
### Incoming Stack Overflow Questions
* Normalize, clean, and tokenize the question in some way
* Index incoming questions by tag (i.e. Javascript, Python)
* For each tag, use Min-Hashing and Locality Sensitive Hashing (Jaccard Similarity) to bucket similar questions
* Use an additional similarity metric + feedback from flagged questions to determine potential duplicates

### Incoming Flagging Events for Stack Overflow Questions
* Separate flagging events into those flagged by ordinary users and those flagged by moderators/high reputation users (for simulation's sake)
* Use flagged "duplicate" questions as a source of truth -> Normalize, clean, and tokenize this question
* Obtain some kind of metric - to be determined at the moment - that offers insight for recognizing a duplicate questions ( this might be on the more machine learning side of things and add unnecessary complexity? still need to flesh this out)
* Store these mysterious metrics in Redis - make available for query by the question pipeline

### User Interface
* Allow duplicate questions to be sorted by tag
* Display mini graphs for potential duplicate questions, where nodes represent questions and edges connect potential duplicate quesitons. 
	* Edge weights represent the similarity between two questions, and the level of color saturation for a node represents the popularity(upvotes) of the question. 
	* We would like to identify the root question, or original question (such that all the other questions would then be considered duplicate questions) by the usefulness of the question, which seems to be best captured by popularity

### Futher thoughts
* Stack Overflow provides the *related_post* attribute for every post, determined by their internal data processing pipeline (specifically [Elastic Search's "More Like This" query](https://meta.stackexchange.com/questions/20473/how-are-related-questions-selected)). This allows us to generate a graph where nodes represent questions and edges connect related questions - could this be used for anything?

### Architecture
![Architecture](https://raw.github.com/kellielu/askedagain/master/imgs/AnnotatedArchitecture.jpg)
### Dataset
Stack Overflow data dump, available as a subset of the [Stack Exchange data dump](https://archive.org/details/stackexchange). 
The Stack Overflow dataset is also accessible on [Google Big Query](https://cloud.google.com/bigquery/public-data/stackoverflow).
## References
[1] [Deduplication in massive clinical notes dataset](https://arxiv.org/pdf/1704.05617.pdf)