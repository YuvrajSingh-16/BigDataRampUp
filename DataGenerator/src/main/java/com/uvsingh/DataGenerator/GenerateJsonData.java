package com.uvsingh.DataGenerator;

import org.json.JSONObject;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.time.Instant;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import java.text.SimpleDateFormat;  
import java.util.Date;  

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class GenerateJsonData {
	
	// File locations & URLs
	static String HDFS_URL, CLICKSTREAM_HOME, CLICKSTREAM_HOME_HDFS;
	static String DATA_DIR, LISTDATA_PATH;
	
	// Random values list
	static List<String> items, paymentMethods, actions, users, locations;
	// Random object
	static Random random = new Random();
	
	
	public static void main(String[] args) {
		
		String ENV_FILENAME = args[1]+".env";
		// Fetching Env Variables
		Dotenv env = Dotenv.configure()
							.directory(args[0]+"/envs")
							.filename(ENV_FILENAME)
							.load();
		
		int RECORD_SIZE = Integer.parseInt(env.get("RECORD_SIZE"));
		HDFS_URL = env.get("HDFS_URL");
		CLICKSTREAM_HOME = env.get("CLICKSTREAM_HOME");
		CLICKSTREAM_HOME_HDFS = env.get("CLICKSTREAM_HOME_HDFS");
		
		DATA_DIR = CLICKSTREAM_HOME_HDFS + "/data/";
		LISTDATA_PATH = CLICKSTREAM_HOME + "/listsData/";
		
		
		// Loading data from text files into Lists obj
		loadDataIntoLists();
		
		try {
			// Creating Configuration Object
			Configuration configuration = new Configuration();
			
			// Configuration for HDFS storage
			configuration.set("fs.hdfs.impl", 
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		    );
			
			// Configuration for Local FileSystem storage
			configuration.set("fs.file.impl",
		        org.apache.hadoop.fs.LocalFileSystem.class.getName()
		    );
			
			// Configuring properties like - FS, resources(XMLs), replication 
	        configuration.set("fs.defaultFS", HDFS_URL);
	        configuration.addResource(new Path("$HADOOP_HOME/conf/core-site.xml"));
	        configuration.addResource(new Path("$HADOOP_HOME/conf/hdfs-site.xml"));
	        configuration.set("dfs.replication","1");
	        
	        // FileSystem obj using above configurations
	        FileSystem fileSystem = FileSystem.get(configuration);
	        
	        // DateFormatter 
	        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy_HH");  
	        Date date = new Date();
	        
	        // JSON fileName
	        String fileName = "clickstreamJson_"+formatter.format(date)+".json";
	        //fileName = "clickstreamJson_03-11-2022_4.json";
	        Path hdfsWritePath = new Path(DATA_DIR + fileName);
	        
	        // OutputStream & BufferWriter object
	        OutputStream os = fileSystem.create(hdfsWritePath);
        	BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter( os, "UTF-8" ));
        	
        	// Generating rows
	        for(int i=0; i<RECORD_SIZE; i++) {
		        bufferedWriter.write(getJSON().toString());
		        bufferedWriter.newLine();
	        }

	        bufferedWriter.close();
	        fileSystem.close();

	        // Saving JSON to Local FileSystem
//			BufferedWriter file = new BufferedWriter(new FileWriter("newclickstream_Data.json"));
//			
//			for(int i=0; i<1000; i++) {
//				file.write(getJSON().toString());
//				file.newLine();
//			}
//	        file.close();

	      } catch (IOException e) {
	    	  e.printStackTrace();
	    	  System.exit(0);
	    }

		System.out.println("[+] "+RECORD_SIZE+" Rows Data Generation Done!!!");
		System.out.println("[+] Completed!");
	}
	
	// Returns ArrayList<String> converted from txt file
	@SuppressWarnings("resource")
	static ArrayList<String> listFromFile(String path){
		Scanner scanner;
		ArrayList<String> list = new ArrayList<String>();
		try {
			// Scanning using ',' as delimiter
			scanner = new Scanner(new File(path)).useDelimiter(",");
			
			while (scanner.hasNext()){
				String str = scanner.next().trim();
			    list.add(str);
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return list;
	}
	
	// Imports All the random values list from txt file
	static void loadDataIntoLists() {
		items = listFromFile(LISTDATA_PATH+"/items.txt");
		
		paymentMethods = listFromFile(LISTDATA_PATH+"/paymentMethods.txt");
		
		actions = new ArrayList<>(Arrays.asList("view", "addtocart", "removefromcart", "purchase"));
		
		users = listFromFile(LISTDATA_PATH+"/users.txt");
		
		locations = listFromFile(LISTDATA_PATH+"/locations.txt");
	}

	
	public static Instant between(Instant startInclusive, Instant endExclusive) {
	    long startSeconds = startInclusive.getEpochSecond();
	    long endSeconds = endExclusive.getEpochSecond();
	    long random = ThreadLocalRandom
	      .current()
	      .nextLong(startSeconds, endSeconds);

	    return Instant.ofEpochSecond(random);
	}
	
	public static String getRandomDateTime() {
		Instant sixMonthsAgo = Instant.now().minus(Duration.ofDays(1 * 182));
		Instant tenDaysAgo = Instant.now().minus(Duration.ofDays(10));
		Instant randomDate = between(sixMonthsAgo, tenDaysAgo);
		
		java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		
		return format.format(randomDate.toEpochMilli());
	}
	
	// Return JSONObject of randomly generated data
	public static JSONObject getJSON() {
		JSONObject obj = new JSONObject();
		
		// Setting all the fields of required JSON Object
		obj.put("userID", users.get(random.nextInt(users.size())));

		obj.put("location", locations.get(random.nextInt(locations.size())));

		long id = 1000000000 + random.nextInt(1000000000);
		obj.put("sessionId", "Session_clickShop"+id);
		
		String action = actions.get(random.nextInt(actions.size()));
		
		int item_idx = random.nextInt(items.size());
		String url = "http://www.shop.com/" + action + "/item?";
		url += items.get(item_idx);
		
		obj.put("action", action);
		
		obj.put("url", url.toString());
		
		String dateTime = getRandomDateTime()+"."+String.valueOf(random.nextInt(900) + 100);
		obj.put("logTime", dateTime);

		/// If not purchased then set paymentMethod NULL
		if(action.equals("purchase")) {
			int idx = random.nextInt(paymentMethods.size());
			obj.put("payment_method", paymentMethods.get(idx));
		}else {
			obj.put("payment_method", "NA");
		}

		obj.put("logDate", dateTime.substring(0, 10));
		
		return obj;
	}
}
