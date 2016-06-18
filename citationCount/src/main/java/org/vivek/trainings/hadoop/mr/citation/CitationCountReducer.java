package org.vivek.trainings.hadoop.mr.citation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CitationCountReducer extends Reducer<Text, Text, Text, Text> {

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
		String csv = "";
		for (Text val : values) {
			if (csv.length() > 0)
				csv += ",";
			csv += val.toString();
		}

		context.write(key, new Text(csv));
	}

}
