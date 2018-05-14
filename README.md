## AskedAgain
A real-time duplicate question suggestion pipeline for Stack Overflow.

### Motivation
Stack Overflow is one of the premium information exchange sites for programmers to find answers to coding questions. However, the site is often plagued with duplicate questions that generate a messy web of answers for users on the site - creating mild (oft unnecessary) confusion, wasting time and energy, and deteriorating the quality of Stack Overflow's general knowledge base. 

Despite [recognizing this problem](https://stackoverflow.blog/2009/04/29/handling-duplicate-questions/), at the present, Stack Overflow still depends upon **manual intervention** from a small percentage of users - moderators and users with high reputation - to identify and mark duplicate questions.

AskedAgain was motivated by a desire to create a **real-time duplicate question system** to assist this process and to explore the efficacy of using **dimensionality reduction techniques such as MinHashLSH** to accelerate text comparison performance.
 

## Implementation Overview
#### Preprocessing
* Stack Overflow questions are first preprocessed into question text bodies. 
	*  **question text body**: question title concatenated with the question body
	*  Each question text body is tokenized, stripped of stop words, and stemmed. 

#### Dimensionality Reduction - MinHashLSH, Stack Overflow Tag Indexing
* Questions are **indexed by tag** (i.e. Javascript, Python), and hashed to Redis Sorted Sets by popularity
* Using **a custom implementation of MinHashLSH**, the MinHash and Locality Sensitive Hash (Jaccard Similarity) for every question is computed to reduce the dimensionality of the question text body
* **Pairwise comparisons** of questions' Locality Sensitive Hashes are then performed within tags (rather than across the entire corpus)

#### Duplicate Question Candidate Identification
* If two question text bodies share buckets from Locality Sensitive Hash comparisons, we can then use **a similarity metric** to determine if they are a duplicate question candidate
* For this use case, we used MinHash Jaccard similarity as this similarity metric. 
* However, provided the large body of NLP and Machine Learning research in the field of semantic similarity and duplicate document detection, this metric could be replaced with a more suitable model for better performance. 


### Architecture
![Architecture](https://raw.github.com/kellielu/askedagain/master/imgs/Architecture.jpg)
### Dataset
Stack Overflow data dump, available as a subset of the [Stack Exchange data dump](https://archive.org/details/stackexchange). 
The Stack Overflow dataset is also accessible on [Google Big Query](https://cloud.google.com/bigquery/public-data/stackoverflow).

## Engineering Challenges
###Verifying custom MinHashLSH implementation
![MinHashLSH_Batch_Benchmark](https://raw.github.com/kellielu/askedagain/master/imgs/MinHashLSH_Batch_Benchmark.jpg)

### Streaming Thoroughput
![MinHashLSH_Streaming_Benchmark](https://raw.github.com/kellielu/askedagain/master/imgs/MinHashLSH_Streaming_Benchmark.jpg)

### Visualization
[Demo](https://youtube.com)

## Conclusions and Further Thoughts


## References
[1] [Deduplication in massive clinical notes dataset](https://arxiv.org/pdf/1704.05617.pdf)