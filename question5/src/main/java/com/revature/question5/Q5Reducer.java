package com.revature.question5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Q5Reducer extends Reducer<Text, Text, Text, Text>{
	//private static Logger log = LogManager.getLogger(FemGradReducer.class);
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
