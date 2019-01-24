package com.revature.question1.FemGrad;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

public class FemGradReducer extends Reducer<Text, Text, Text, Text>{
	/**
	 * Question 1 Reducer
	 * <p>
	 * 	The Question 1 Reducer receives the key, value pair and then splits the value pair
	 * in to year and data. the data is less than 30% it is written to the context. if the
	 * year is -1 then the Text "No Recent Data" is written instead.
	 * </p>
	 * 
	 * 
	 */
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException{
		//log.traceEntry();
		for (Text val : values) {
			String[] strArr = val.toString().split("%%");
			int year = Integer.parseInt(strArr[0]);
			double value = Double.parseDouble(strArr[1]);
			if(value < 30.0 && year != -1) {
				context.write(key, val);
			} else if (year == -1) {
				context.write(key, new Text("No Recent Data"));
			}
		}
	}
}

