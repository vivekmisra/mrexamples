package org.vivek.trainings.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WordCountReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {
	   
/*@Override
public void reduce(Text key, Iterable<IntWritable> values,
    Context context)
    throws IOException, InterruptedException {
  
	 int sum = 0;
     for (IntWritable val : values) {
       sum += val.get();
     }
     result.set(sum);
	  context.write(key, result);
}
*/
private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		           throws IOException, InterruptedException {
	/*
	 *Reduce : input (K2, list(V2))  
	 * In reduce, you get  input with each shuffled sorted distinct key K2 and list of values V2,
	 * So ydeveloper will loop each input value list(V2) and apply business logic . In wordcount, you will define  output 
	 * as list of words and their ddistictive sum (counts)
	 * Reduce : output
	 * list(K3,V3) = list(K3 :each distinctive word in different reducers writable, V3:sum of counts of V2 if they fall in same reducer)
	 */
  int sum = 0;
  for (IntWritable val : values) {
    sum += val.get();
  }
  result.set(sum);
  context.write(key, result);
}
}
