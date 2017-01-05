# Inversed Indexing with MapReduce

## 1. What the program does

Inversed indexing is a batch processing to sort documents for each popular keyword, so that it makes easier and faster for document-retrieval requests to find documents with many hits for a set of user-given keywords. For instance, given a request for finding documents with term1, term2, and term3, we will perform intersection of the corresponding three lists of postings and realize that document 11 includes all of these keywords.

The main( `String[] args` ) function receives keywords or terms in args. As an output, the program creates a list of
documents for each keyword sorted by the number of occurences of the given keyword in the document.

## 2. Local testing

**Prerequisites:** installed Java 1.8 and Maven

1. install Hadoop with homebrew: `brew install hadoop`
2. check version of hadoop: `brew info hadoop`
3. check that pom.xml has the same version of hadoop client
4. run `mvn package` to compile the project into a jar file
5. run the application over a test set of data in a standalone mode `hadoop jar target/inv-indexes-1.0-SNAPSHOT.jar kosiachenko.InvIndexes hdfs/input hdfs/output rfc UDP`
6. check the hdfs/output/part-00000 file to see the results

## 3. Hadoop and MapReduce Installation

After testing the program locally, I installed the Hadoop File System on the cluster of 4 machines (including configuring the YARN resource-management platform) for testing on an actual cluster.

I also copied the input files (169 files containing the IETF (Internet Engineering Task Force) protocol standards and draft standards) into the HDFS.

## 4. Implementation details

The workflow of my program is as follows:

<img src="https://github.com/kosiachenko/InvertedIndexer_MapReduce/blob/master/images/program_flow.png" alt="Program flow"> 

I also implemented sorting of document by the number of keyword occurences in that document in my reducer function.

## 5. Issues encountered

The main issue I encountered was with the Combiner. After spending many hours trying to debug the mysterious behavior of my program I figured out what was the problem. The code of the main() function I coppied from the spec includes the following line:

`conf.setCombinerClass(Reduce.class);`

It means that we specify our Reduce class also as our Combine class. According to the Internet, Combiner is an optional class in a MapReduce program which operates by accepting the inputs from the Map class and thereafter passing the output key-value pairs to the Reducer class. Because my initial design of the program didn’t expect for Reducer output to be passed as input back to the Reducer, it didn’t work as expected and produced wrong results. The workflow was basically as follows:

<img src="https://github.com/kosiachenko/InvertedIndexer_MapReduce/blob/master/images/program_flow_issue.png" alt="Issue in program flow">

I figured I can solve this issue in several different ways:

1) Delete the line in main() where we set the Reduce.class as our Combiner. This was what I initially did, but than I realized that this could lead to a sub-optimal performance.

2) Handle Combiner output in Reduce class as a valid input. This is __what I have__ in my final program. Returning the Combiner __improved program performance__ by 1,16 times for the Yarn-version (from 175 sec to 150 sec).

3) Write my own Combiner class. This seemed like an overkill in this case.

## 6. Performance analysis

I didn't observe any performance speed-up from parallelizing the job over 4-machine cluster compared to the local job. The completion of the task actually took longer in that case.

But these results are expected, because I worked with a very small dataset (on the Hadoop scale), so the network communication overhead was way bigger than any performance gains from parallelization.
