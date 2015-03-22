package edu.uic.ids561.aramna2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MovieReviewMapperTwo extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		// parse the key and store it in an array
		String[] values = value.toString().split("\\^");
		String[][] movieWithRating = new String[values.length][2];
		
		for(int i=0 ; i<values.length ; i++){
			movieWithRating[i][0] = values[i].split(",")[0];
			movieWithRating[i][1] = values[i].split(",")[1];
		}
		
		for(int i=0 ; i<movieWithRating.length-1 ; i++){
			for(int j=i+1 ; j<movieWithRating.length ; j++){
				String[] movieList = {movieWithRating[i][0], movieWithRating[j][0]};
				
				String temp = movieList[0];
				
				Arrays.sort(movieList);
				
				if(temp.equals(movieList[0])){
					output.collect(new Text(movieList[0]+","+movieList[1]), new Text(movieWithRating[i][1]+","+movieWithRating[j][1]));
				}
				else{
					output.collect(new Text(movieList[0]+","+movieList[1]), new Text(movieWithRating[j][1]+","+movieWithRating[i][1]));
				}
			}
		}
	}
}
