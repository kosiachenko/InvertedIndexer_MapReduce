package kosiachenko;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

/**
 * Created by elizaveta on 27/12/16.
 */
public class InvReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
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
