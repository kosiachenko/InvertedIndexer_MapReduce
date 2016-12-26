# Inversed Indexing with MapReduce

## 1. What the program does

Inversed indexing is a batch processing to sort documents for each popular keyword, so that it makes easier and faster for document-retrieval requests to find documents with
many hits for a set of user-given keywords. For instance, given a request for finding documents with term1,
term2, and term3, we will perform intersection of the corresponding three lists of postings and realize that
document 11 includes all of these keywords.

The main( `String[] args` ) function receives keywords or terms in args. As an output, the program creates a list of
documents for each keyword sorted by the number of occurences of the given keyword in the document.

## 2. Hadoop and MapReduce Installation

Prior to running this program, I installed the Hadoop File System on the cluster of 4 machines (including configuring the YARN resource-management platform).
I also copied the input files (169 files containing the IETF (Internet Engineering Task Force) protocol standards and draft standards) into the HDFS.

## 3. Implementation details


## 4. issues encountered


## 5. Performance analysis

I didn't observe any performance speed-up from parallelizing the job over 4-machine cluster compared to the local job. The completion of the task actually took longer in that case.
But these results are expected, because I worked with a very small dataset (on the Hadoop scale), so the network
communication overhead was way bigger than any performance gains from parallelization.
