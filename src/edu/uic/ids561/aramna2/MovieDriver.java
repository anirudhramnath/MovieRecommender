package edu.uic.ids561.aramna2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;

public class MovieDriver {

	private static final String INPUT_FOLDER_NAME = "input";
	private static final String OUTPUT_FOLDER_NAME = "output";

	public static void main(String[] args) {

		JobClient client = new JobClient();
		JobConf conf;

		int iterationCount = 0;

		while (++iterationCount <= 2) {
			 			
			conf = new JobConf(edu.uic.ids561.aramna2.MovieDriver.class);
			
			//Input format
			if(2 == iterationCount){
				conf.setInputFormat(KeyValueTextInputFormat.class);
			}
			
			// specify output types
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			
			String input, output;

			if(1 == iterationCount){
				input = INPUT_FOLDER_NAME;
			}
			else{
				input = OUTPUT_FOLDER_NAME+(iterationCount-1);
			}
			
			output = OUTPUT_FOLDER_NAME+iterationCount;

			// specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			// specify a mapper
			if(1 == iterationCount)
				conf.setMapperClass(MovieReviewMapperOne.class);
			else
				conf.setMapperClass(MovieReviewMapperTwo.class);
			
			// specify a reducer
			if(1 == iterationCount)
				conf.setReducerClass(MovieReviewReducerOne.class);
			else
				conf.setReducerClass(MovieReviewReducerTwo.class);

			client.setConf(conf);
			try {
				JobClient.runJob(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
