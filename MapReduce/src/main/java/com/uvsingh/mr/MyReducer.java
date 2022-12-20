package com.uvsingh.mr;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/*
 * apple = [1, 1, 1, 1, 1]
 */

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	Logger logger = Logger.getLogger(MyMapper.class.getName());
	
	public MyReducer() {
		logger.info("[+] MyReducer Object Created.");
	}

	@Override
	protected void reduce(Text word, Iterable<IntWritable> counts,
			Context context) throws IOException, InterruptedException {
	
		int total=0;
		for(IntWritable iw: counts) {
			total += iw.get();
		}
		
		// Writing into context object
		context.write(word, new IntWritable(total));
	}
}
