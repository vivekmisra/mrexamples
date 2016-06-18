package org.vivek.trainings.hadoop.mr.citation;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CitationCountMapper  extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                    
        String[] citation = value.toString().split(",");//split V1 by "," and get first cited [1] as K2 
        context.write(new Text(citation[1]), new Text(citation[0]));// list(K2,V2)
    }
	      
	   
	
}
