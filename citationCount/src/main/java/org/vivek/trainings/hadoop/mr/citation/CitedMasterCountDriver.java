package org.vivek.trainings.hadoop.mr.citation;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*This MASTER class is responsible for running map reduce job*/
public class CitedMasterCountDriver extends Configured implements Tool{
	
	
	public static class CitedMapperCount  extends Mapper<LongWritable, Text, Text, Text> {
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	                    
	        String[] citation = value.toString().split(",");//split V1 by "," 
	        //Now get  [1] "Cited" as K2 and [0] "Citing" as V2 ,reverse index
	        context.write(new Text(citation[1]), new Text(citation[0]));// list(K2,V2)
	    }
		      
		   
		
	}
	
	public static class CitedReducerCount extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/*
			 * Reduce : input (K2, list(V2)) In reduce, you get input with each
			 * shuffled sorted distinct key K2 and list of values V2, So you will
			 * loop each input value list(V2) and apply business logic . In this
			 * example, you will define output as list of K3,V3 which is list of K2
			 * and comma separted values V2 Now V2 consists of citings and there an
			 * be more than one citings which point to cited K2 . Hence, you loop
			 * thru V2 for a particular K2 and append by comma separated for nextr
			 * V2 for same K2 if it exists. Reduce : output list(K3,V3) = list(K2
			 * [each distinctive cited ]:comma separated V2 in each reducer task)
			 */
			int size = 0;//List V2 , get V3 by iterating over V2'
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
			  it.next();
			  size++;
			}
			//V3 is of  IntWritable type
			 IntWritable value = new IntWritable(size);
		
		

			context.write(key, value);
		}
	}


	public int run(String[] args) throws Exception
	{
	
		Configuration conf = getConf();
        
        Job job = new Job(conf, "MyJob");
        job.setJarByClass(CitedMasterCountDriver.class);
        
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setMapperClass(CitedMapperCount.class);
        job.setReducerClass(CitedReducerCount.class);
        
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        System.exit(job.waitForCompletion(true)?0:1);
        
        return 0;
	}

	public static void main(String[] args) throws Exception {
		CitedMasterCountDriver driver = new CitedMasterCountDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}
