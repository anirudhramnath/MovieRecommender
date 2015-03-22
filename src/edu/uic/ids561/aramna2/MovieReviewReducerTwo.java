package edu.uic.ids561.aramna2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MovieReviewReducerTwo extends MapReduceBase implements Reducer<Text, Iterator<Text>, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Iterator<Text>> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
			
		int cnt = 0;
		
		double cosineSimilarity, magnitude_x=0, magnitude_y=0;
		int dotProduct = 0;
		
		while(values.hasNext()){
			
			Text currentVal = (Text) values.next();
			
			String[] ratingValues = currentVal.toString().split(",");

			int currentDotProduct = 1;
			
			for(String s:ratingValues){
				currentDotProduct *= Integer.parseInt(s);
			}
			
			// calculating the dot product
			dotProduct += currentDotProduct;
			
			// calculating the magnitudes
			magnitude_x += Integer.parseInt(ratingValues[0]) * Integer.parseInt(ratingValues[0]);
			magnitude_y += Integer.parseInt(ratingValues[1]) * Integer.parseInt(ratingValues[1]);
			
			cnt ++;
			
		}
		
		// write to output only if more than one pair of ratings is found
		if(cnt > 1){
			// calculating cosine similarity
			cosineSimilarity =  dotProduct;
			cosineSimilarity /= Math.sqrt(magnitude_x)*Math.sqrt(magnitude_y);
			output.collect(key, new Text(String.valueOf(cosineSimilarity)));
		}
		
	}

}
