package org.vivek.trainings.e63;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordCountMapper  extends Mapper<Object, Text, Text, IntWritable>{

	private static final String DELIMITER = " \t\n\r\f,.:;?![]'";
	
	  private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	      
	   
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		/*
		 * Map : input (K1,V1) = (new line index, line) 
		 * In mapper , read each input value V1(line) .Developer define a key specified as part of business
		 * logic . In wordcount, you will define K2 as each word, so you to iterate over input value V1 to extract key K2 by defined
		 * delimeter and you assign a writable counter of 1  as value V2 
		 * Map : output
		 * list(K2,V2) = list(K2 :each word, V2:count as 1)
		 */

		StringTokenizer itr = new StringTokenizer(value.toString(), DELIMITER);
		while (itr.hasMoreTokens()) {//Iterate over V1 to get K2 as next token
			word.set(itr.nextToken());// construct K2 from V1 by iterating over V1 with defined delimeter
			context.write(word, one);// list(K2,V2)
		}
	}
}
