package com.revature.question4;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Q4Mapper extends Mapper <LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		/* 
		 * 	Convert line (Text) to a String array where each "cell" is array cell.
		 * */
		String line = value.toString();
		String[] values = line.split("\",\"?", -1);
		
		
		String search = "SL.EMP.TOTL.SP.FE.ZS";
		String country; 
		int lookBack = 17; // how far you want to look back 
		
		if (values[3].equals(search)) {
			values = Arrays.copyOf(values, 61);
			country = values[0].substring(1); //clean the name of the country
			int length = values.length;
			YearData yd = null;
			
			
			newData:for (int i = 1; i <= lookBack; ++i) { // look back over the last 5 years
				String holder = values[length - i];
				int year = 2017 - i; // 2016+1
				if(holder != null && !holder.isEmpty()) {
					double data = Double.parseDouble(holder);
					yd = new YearData(year, data);
					Text outData = new Text(yd.toString());
					context.write(new Text(country), outData);
					break newData;
				}				
			}
			oldData: for(int i = 0; i < lookBack; ++i) {
				String holder = values[length - lookBack];
				int year = 2000 + i;
				if(holder != null && !holder.isEmpty()) {
					double data = Double.parseDouble(holder);
					yd = new YearData(year, data);
					Text outData = new Text(yd.toString());
					context.write(new Text(country), outData);
					break oldData;
				}
			}
		}		
	}
}
