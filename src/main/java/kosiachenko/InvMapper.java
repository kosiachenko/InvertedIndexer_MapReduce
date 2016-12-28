package kosiachenko;

/**
 * Created by elizaveta on 27/12/16.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class InvMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private JobConf conf;
    private Text keyword = new Text();
    private Text filename_count = new Text();

    public void configure( JobConf job ) {
        this.conf = job;
    }

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
