package com.uvsingh.mr;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.*;

/*
 * 0 apple ball apple
 * 17 ball apple apple
*/

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	IntWritable one = new IntWritable(1);
	
	Logger logger = Logger.getLogger(MyMapper.class.getName());
	
	public MyMapper() {
		logger.info("[+] Mapper Object Created.");
	}


	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String location;
		String line = value.toString();
		String[] tuple = line.split("\\n");
		
		// Fetching location field from JSON object & counting those
        try{
            for(int i=0;i<tuple.length; i++){
                JSONObject obj = new JSONObject(tuple[i]);
                location = obj.getString("location");
                context.write(new Text(location), one);
            }
        }catch(JSONException e){
            e.printStackTrace();
        }
		
	}
}

