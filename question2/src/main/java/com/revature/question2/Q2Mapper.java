package com.revature.question2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class Q2Mapper extends Mapper <LongWritable, Text, Text, Text>{ 
	
	@Override 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				
		/* 
		 * 	Convert line (Text) to a String array where each "cell" is array cell.
		 * */
		String line = value.toString();
		String[] values = line.split("\",\"?", -1);
				
		String search = "SE.TER.CMPL.FE.ZS"; // Tertiary education, gross completion ratio, female
		String countryCode = "USA";
		int lookBack = 17;
		
		if (values[1].equals(countryCode) && values[3].equals(search)) {
			
			values = Arrays.copyOf(values, 61);
			int length = values.length;
			for (int i = 1; i <= lookBack; ++i) { 
				String holder = values[length - i];
				if(holder != null && !holder.isEmpty()) {  // if data
					// list is (year, datum)
					int year = 2017 - i; 
					double data = Double.parseDouble(holder);
					YearData yd = new YearData(year, data);
					Text outData = new Text(yd.toString());
					context.write(new Text(countryCode), outData);
				} 
			}
		}
	}
}