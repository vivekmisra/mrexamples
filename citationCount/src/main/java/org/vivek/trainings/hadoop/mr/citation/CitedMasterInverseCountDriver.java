package org.vivek.trainings.hadoop.mr.citation;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*This MASTER class is responsible for running map reduce job*/
public class CitedMasterInverseCountDriver {

	public static class CitedMapperCount extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] citation = value.toString().split(",");// split V1 by ","
			// Now get [1] "Cited" as K2 and [0] "Citing" as V2 ,reverse index
			context.write(new Text(citation[1]), new Text(citation[0]));// list(K2,V2)
		}

	}

	public static class CitedReducerCount extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/*
			 * Reduce : input (K2, list(V2)) In reduce, you get input with each
			 * shuffled sorted distinct key K2 and list of values V2, So you
			 * will loop each input value list(V2) and apply business logic . In
			 * this example, you will define output as list of K3,V3 which is
			 * list of K2 and comma separted values V2 Now V2 consists of
			 * citings and there an be more than one citings which point to
			 * cited K2 . Hence, you loop thru V2 for a particular K2 and append
			 * by comma separated for nextr V2 for same K2 if it exists. Reduce
			 * : output list(K3,V3) = list(K2 [each distinctive cited ]:comma
			 * separated V2 in each reducer task)
			 */
			int size = 0;// List V2 , get V3 by iterating over V2'
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				it.next();
				size++;
			}
			// V3 is of IntWritable type
			IntWritable value = new IntWritable(size);

			context.write(key, value);
		}
	}

	public static class InverseMapper<K, V> extends Mapper<K, V, V, K> {

		/** The inverse function. Input keys and values are swapped. */
		@Override
		public void map(K key, V value, Context context) throws IOException, InterruptedException {
			context.write(value, key);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: CitedMasterInverseCountDriver  <in> <out>");
			System.exit(2);
		}

		Path tempDir = new Path("Cited-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		Job job = new Job(conf, "word count");
		job.setJarByClass(CitedMasterInverseCountDriver.class);

		job.setMapperClass(CitedMapperCount.class);
		// job.setCombinerClass(CitedReducerCount.class);
		job.setReducerClass(CitedReducerCount.class);

		// The map output key k2 value V2 are of Text,Text type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// The reducer output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, tempDir);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		if (job.waitForCompletion(true)) {
			Job sortJob = new Job(conf, "sort");
			sortJob.setJarByClass(CitedMasterInverseCountDriver.class);

			FileInputFormat.addInputPath(sortJob, tempDir);
			sortJob.setInputFormatClass(SequenceFileInputFormat.class);

			sortJob.setMapperClass(InverseMapper.class);
			sortJob.setMapOutputKeyClass(IntWritable.class);
			sortJob.setMapOutputValueClass(Text.class);
			sortJob.setNumReduceTasks(1);

			FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[1]));

			sortJob.setOutputKeyClass(IntWritable.class);
			sortJob.setOutputValueClass(Text.class);
			System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
		}
		FileSystem.get(conf).deleteOnExit(tempDir);

	}

}
