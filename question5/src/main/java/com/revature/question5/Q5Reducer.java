package com.revature.question5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Q5Reducer extends Reducer<Text, Text, Text, Text>{
	/**
	 * Question 5 Reducer
	 * <p>
	 * 	The Question 5 Reducer receives the key, value pair and then splits the value pair
	 * in to year and data. the data is less than 30% it is written to the context. if the
	 * year is -1 then the Text "No Recent Data" is written instead.
	 * </p>
	 * 
	 * 
	 */
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException{
		int sum = 0;
		double gdp = 0;
		for(Text val : values) {
			String str = val.toString();
			String[] strArr = str.split("%%");
			if (strArr[0].equals("1")) {
				sum++;
			} else {
				gdp = Double.parseDouble(strArr[1]);
			}
		}
		String outStr = "" + sum + "\t" + gdp;
		context.write(key, new Text(outStr));	
	}
}
