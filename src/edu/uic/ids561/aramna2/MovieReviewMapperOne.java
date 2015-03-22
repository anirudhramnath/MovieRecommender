package edu.uic.ids561.aramna2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MovieReviewMapperOne extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String returnKey, returnValue;
		
		// parse the key and store it in an array
		String[] values = value.toString().split(",");
		
		returnKey = values[0];
		returnValue = values[1] + "," + values[2];
		
		output.collect(new Text(returnKey), new Text(returnValue));
	}
}
