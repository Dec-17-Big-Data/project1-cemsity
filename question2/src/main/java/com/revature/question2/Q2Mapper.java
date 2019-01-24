package com.revature.question2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class Q2Mapper extends Mapper <LongWritable, Text, Text, Text>{ 
	
	
	/**
	 * Question 2 Mapper
	 * <p>
	 * For Question 2, map iterates over the document looking for lines that 
	 * contains the Series code. I chose tertiary education, gross completion ratio because 
	 * it is close measurement to yearly graduation and thus a timely measurement and the contry
	 * code of the United States. 
	 * Once the line is selected the mapper starts at the end and checkes each cell until 
	 * the year 2000 as per requirements
	 * After selecting data, the mapper writes the country as the key, and the year and 
	 * percentage, in a string separated by double %. 
	 *
	 * </p>
	 * 
	 * @param key		the LongWritable key assigned by the splitter
	 * @param text		the Text Value that contains one line of the csv
	 * @param context	the Context object
	 * 
	 */
	@Override 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				
		
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