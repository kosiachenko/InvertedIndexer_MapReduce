import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class InvIndexes {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		JobConf conf;
		public void configure( JobConf job ) {
			this.conf = job;
		}
		private Text keyword = new Text();
		private Text filename_count = new Text();

		public void map(LongWritable docId, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int argc = Integer.parseInt( conf.get( "argc" ) );
			FileSplit fileSplit = ( FileSplit )reporter.getInputSplit( );
			String filename = "" + fileSplit.getPath( ).getName( );

			HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
			for (int i = 0; i<argc; i++) {
				hashmap.put(conf.get("keyword" + i), 0);
			}

			String line = value.toString();
		    StringTokenizer tokenizer = new StringTokenizer(line);
		    while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				if (hashmap.containsKey(word)) {
					hashmap.put(word, hashmap.get(word)+1);
				}
			}
		
			for (HashMap.Entry<String, Integer> entry : hashmap.entrySet()) {
				if (entry.getValue() != 0) {
					keyword.set(entry.getKey());
					String str = "" + filename +" " + entry.getValue();
					filename_count.set(str);
					output.collect(keyword, filename_count);
				}
			}
		}
	}

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		    Text docListText = new Text();
            String docListString = "";
			
			HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
			
			while (values.hasNext()) {
			    Text value = values.next();
			    String line = value.toString();
			    
		        StringTokenizer tokenizer = new StringTokenizer(line);
		        
		        while (tokenizer.hasMoreTokens()) {
				    String filename = new String(tokenizer.nextToken());
				    if (tokenizer.hasMoreTokens()) {
				        int count = Integer.parseInt(tokenizer.nextToken());
				        int curVal = hashmap.get(filename)!= null ? hashmap.get(filename) : 0;
				        hashmap.put(filename, curVal + count);
				    }
				}
           	}
           	
           	//Sorting documents by number of occurences of keyword:
           	
            List<HashMap.Entry<String, Integer>> entries = new LinkedList<HashMap.Entry<String, Integer>>(hashmap.entrySet());
            Collections.sort(entries, new Comparator<HashMap.Entry<String, Integer>> ()
            {
                public int compare(HashMap.Entry<String, Integer> entry1, HashMap.Entry<String, Integer> entry2) {
                    return (entry2.getValue().compareTo(entry1.getValue()));
                
                }
            });
            
            for (HashMap.Entry<String, Integer> entry : entries) {

				docListString += entry.getKey();
				docListString += " ";
				docListString += entry.getValue();
				docListString += " ";
			}
	        
	        docListText.set(docListString);
			output.collect(key, docListText);  //where docListText is a string concatenation of all filename_counts
		}
	}

    public static void main(String[] args) throws Exception {
		// input format:
		// hadoop jar invertedindexes.jar InvertedIndexes input output keyword1 keyword2 ...\
		
		long startT = System.currentTimeMillis();
		
		JobConf conf = new JobConf(InvIndexes.class);
		conf.setJobName("InvIndexes");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); // input directory name
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); // output directory name

		conf.set( "argc", String.valueOf( args.length - 2 ) ); // argc maintains #keywords
		for ( int i = 0; i < args.length - 2; i++ )
			conf.set( "keyword" + i, args[i + 2] ); // keyword1, keyword2, ...

		JobClient.runJob(conf);
		
		long endT = System.currentTimeMillis();
		long elapsedT = endT- startT;
		System.out.println("Time elapsed (in miliseconds): " + elapsedT);
	}
}
