package edu.uic.ids561.aramna2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MovieReviewReducerOne extends MapReduceBase implements Reducer<Text, Iterator<Text>, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Iterator<Text>> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
			
		StringBuffer invertedIndex = new StringBuffer();
		
		// creating inverted index
		while(values.hasNext()){
			Text currentVal = (Text) values.next();
			
			invertedIndex.append(currentVal);
			invertedIndex.append("^");
		}
		
		String returnString = invertedIndex.toString().substring(0, invertedIndex.toString().length()-1);
		
		output.collect(key, new Text(returnString));
	}

}
