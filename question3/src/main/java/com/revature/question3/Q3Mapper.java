package com.revature.question3;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Q3Mapper extends Mapper <LongWritable, Text, Text, Text>{
	/**
	 * Question 3 Mapper
	 * <p>
	 * For Question 1, map iterates over the document looking for lines that 
	 * contains the Series code. I chose Employment to population ratio, 15+, 
	 * male (%) (modeled ILO estimate) because the ILO has a complete data set
	 * that matches well with the nationally supplied data set. 
	 * Once the line is selected the mapper starts at the end and checks each cell until 
	 * the year 2000.
	 * After selecting data, the mapper writes the country as the key, and the year and 
	 * percentage, in a string separated by double %. 
	 *
	 * </p>
	 * 
	 * @param key		the LongWritable key assigned by the splitter
	 * @param text		the Text Value that contains one line of the csv
	 * @param context	the Context object
	 * 
	 * 
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		/* 
		 * 	Convert line (Text) to a String array where each "cell" is array cell.
		 * */
		String line = value.toString();
		String[] values = line.split("\",\"?", -1);
		
		
		String search = "SL.EMP.TOTL.SP.MA.ZS";
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
