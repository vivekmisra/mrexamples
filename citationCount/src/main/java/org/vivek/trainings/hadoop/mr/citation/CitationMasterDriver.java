package org.vivek.trainings.hadoop.mr.citation;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*This MASTER class is responsible for running map reduce job*/
public class CitationMasterDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception
	{
	
		Configuration conf = getConf();
        
        Job job = new Job(conf, "MyJob");
        job.setJarByClass(CitationMasterDriver.class);
        
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setMapperClass(CitedMapper.class);
        job.setReducerClass(CitedReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true)?0:1);
        
        return 0;
	}

	public static void main(String[] args) throws Exception {
		CitationMasterDriver driver = new CitationMasterDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}
