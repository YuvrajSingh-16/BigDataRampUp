package com.uvsingh.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import io.github.cdimascio.dotenv.Dotenv;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;


public class MyDriver {
	
	// Creating logger object
	static Logger logger = Logger.getLogger(MyDriver.class.getName());
	// HDFS URL 
	static String HDFS_URL, CLICKSTREAM_HOME_HDFS;
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		String ENV_FILENAME = args[1]+".env";
		// Fetching Env Variables
		Dotenv env = Dotenv.configure()
							.directory(args[0]+"/envs")
							.filename(ENV_FILENAME)
							.load();
		
		HDFS_URL = env.get("HDFS_URL");
		CLICKSTREAM_HOME_HDFS = env.get("CLICKSTREAM_HOME_HDFS");
		
		String iPath = CLICKSTREAM_HOME_HDFS+"/data";
		String oPath = CLICKSTREAM_HOME_HDFS+"/mr_output";
		
		// Setting input and output Path
		Path inputPath = new Path(iPath);
		Path outputPath = new Path(oPath);
		
		// Configuration object
		Configuration conf = new Configuration();
		
		// Setting configuration for HDFS
		conf.set("fs.hdfs.impl", 
			org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
	    );
		
		// Setting HDFS URL into configuraiton object & other XMLs resources
		conf.set("fs.defaultFS", HDFS_URL);
		conf.addResource(new Path("$HADOOP_HOME/conf/core-site.xml"));
		conf.addResource(new Path("$HADOOP_HOME/conf/hdfs-site.xml"));
		conf.set("dfs.replication","1");
		
		conf.set("mapreduce.framework.name", "local");
		
		System.out.println("[+] mapreduce.framework set to local");
		
		// Creating job instance using configuration object
		Job job = Job.getInstance(conf);
		
		// Setting mapper, combiner, reducer classes
		job.setMapperClass(MyMapper.class);
		
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);
		
		// Setting jar by class, output key & value class
		job.setJarByClass(MyDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Setting I/P & O/P path into job
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// Deleting output path if exists
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		System.out.println("[+] All configurations are set !!!");
		job.waitForCompletion(true);
		logger.info("MAP-REDUCE ANALYSIS JOB COMPLETED");
		System.out.println("[+] Completed!");
	}
}
