package kosiachenko;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class InvIndexes {
    public static void main(String[] args) throws Exception {
		// input format:
		// hadoop jar invertedindexes.jar InvertedIndexes input output keyword1 keyword2 ...\
		
		long startT = System.currentTimeMillis();
		
		JobConf conf = new JobConf(InvIndexes.class);
		conf.setJobName("InvIndexes");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(InvMapper.class);
		conf.setCombinerClass(InvReducer.class);
		conf.setReducerClass(InvReducer.class);

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
