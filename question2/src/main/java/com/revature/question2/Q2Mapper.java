package com.revature.question2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Q2Mapper extends Mapper <LongWritable, Text, IntWritable, DoubleWritable>{ 
	
	@Override 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				
		/* 
		 * 	Convert line (Text) to a String array where each "cell" is array cell.
		 * */
		String line = value.toString();
		String[] values = line.split("\",\"?", -1);
		
		IntWritable outKey = null;
		DoubleWritable outValue = null;
		
		
		String search = "SE.TER.CMPL.FE.ZS"; // Tertiary education, gross completion ratio, female
		String countryCode = "USA";
		int lookBack = 17;
		
		if (values[1].equals(countryCode) && values[3].equals(search)) {
			int length = values.length;
			values = Arrays.copyOf(values, 61);
			for (int i = 1; i <= lookBack; ++i) { // look back over the last 5 years
				String holder = values[length - i];
				if(holder != null && !holder.isEmpty()) {  // if data
					// list is (year, datum)
					year = new IntWritable(2017 - i); // 2016+1
					
					context.write(outKey, outValue);  //broadcast the data
				} 
			}
		}
	}
}