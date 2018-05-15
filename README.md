## AskedAgain
A real-time duplicate question suggestion pipeline for Stack Overflow.

### Motivation
Stack Overflow is one of the premium information exchange sites for programmers to find answers to coding questions. However, the site is often plagued with **duplicate questions** that generate a messy web of answers for users on the site - creating mild (oft unnecessary) confusion, wasting time and energy, and deteriorating the quality of Stack Overflow's general knowledge base. 

Despite [recognizing this problem](https://stackoverflow.blog/2009/04/29/handling-duplicate-questions/), at the present, Stack Overflow still depends upon **manual intervention** from a small percentage of users - moderators and users with high reputation - to identify and mark duplicate questions.

AskedAgain was motivated by a desire to create a **real-time duplicate question candidate detection system** to assist this process. I also sought to explore and evaluate the efficacy of using **dimensionality reduction techniques such as MinHashLSH** in real-time to accelerate text comparison performance.
 

## Implementation Overview
#### Preprocessing
* Stack Overflow questions are first preprocessed into question text bodies. 
	*  **question text body**: question title concatenated with the question body
	*  Each question text body is tokenized, stripped of stop words, and stemmed. 

#### Dimensionality Reduction - MinHashLSH, Stack Overflow Tag Indexing
* Questions are **indexed by tag** (i.e. Javascript, Python), and hashed to Redis Sorted Sets ordered by popularity
* Using **a custom implementation of MinHashLSH**, the MinHash and Locality Sensitive Hash (Jaccard Similarity) for every question is computed to reduce the dimensionality of the question text body
* **Pairwise comparisons** of questions' Locality Sensitive Hashes are then performed within tags (rather than across the entire corpus)

#### Duplicate Question Candidate Identification
* If two question text bodies share buckets from Locality Sensitive Hash comparisons, we can then use **a similarity metric and similarity threshold** to determine if the pair of questions are potential duplicates
* For this use case, we used MinHash Jaccard similarity as the similarity metric. 
* However, provided the large body of Natural Language Processing and Machine Learning research in the field of semantic similarity and duplicate document detection, this metric could be replaced by a better similarity measure (such as an Machine Learning model) for better performance. 


### Architecture
![Architecture](https://raw.github.com/kellielu/askedagain/master/imgs/Architecture.jpg)
### Dataset
Stack Overflow data dump, available as a subset of the [Stack Exchange data dump](https://archive.org/details/stackexchange). 
The Stack Overflow dataset is also accessible on [Google Big Query](https://cloud.google.com/bigquery/public-data/stackoverflow).

## Engineering Challenges

### Verifying custom MinHashLSH implementation
![MinHashLSH_Batch_Benchmark](https://raw.github.com/kellielu/askedagain/master/imgs/MinHashLSH_Batch_Benchmark.jpg)

### Streaming throughput/latency
![MinHashLSH_Streaming_Benchmark](https://raw.github.com/kellielu/askedagain/master/imgs/MinHashLSH_Streaming_Benchmark.jpg)

## Conclusions and Further Thoughts

### MinHashLSH Model Tuning
* There is a clear tradeoff between time and accuracy for MinHashLSH in respect to choosing k hash tables for MinHash, b bands for LSH, and bw bandwidth for LSH band widths.

### Incremental online MinHashLSH is generally not very scalable
* Sorting questions into tags **increased performance by ~4x** on a sizeable subset of Stack Overflow questions in a benchmark batch process, highlighting the performance increase from indexing questions prior to comparison. 
* Incremental online MinHashLSH **grows in time complexity as dataset size increases.** If the number of comparisons is unregulated and performed naively, the algorithm does not scale well for large datasets even with indexing.
* Generally, rather than an incremental online model, I believe a **windowed streaming model** (i.e. comparing all streaming questions within a window of time) or a **batch model** are more appropriate uses of the MinHashLSH algorithm for general use cases. 

### On duplicate questions
* Duplicate questions are **very sparse in the dataset**, as shown by the low number of questions detected in the sizable subset. This suggests that there may be further avenues to reduce pairwise corpus comparisons for this use case. 
* While pure question deduplication is often a Machine Learning problem where models are trained to detect semantic similarity, MinHashLSH exact similarity (Jaccard similarity) showed **decent accuracy** in identifying near-duplicate questions on questions and their question bodies. 

## References
[1] [Deduplication in massive clinical notes dataset](https://arxiv.org/pdf/1704.05617.pdf)