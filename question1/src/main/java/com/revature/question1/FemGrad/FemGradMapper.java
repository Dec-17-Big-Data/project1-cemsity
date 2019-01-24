package com.revature.question1.FemGrad;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author      Stephen Hilson <stephenrhilson@gmail.com>
 * @version     1.0
 * @since       1.0
 */

public class FemGradMapper extends Mapper <LongWritable, Text, Text, Text>{ 
		
	/**
	 * Question 1 Mapper
	 * <p>
	 * For Question 1, map iterates over the document looking for lines that 
	 * contains the Series code. I chose tertiary education, gross completion ratio because 
	 * it is close mesurment to yearly graduation and thus a timely mesurment.
	 * Once the line is selected the mapper starts at the end and checkes each cell untill 
	 * the year 2012, or the most recent data that is younger than 8 years.
	 * After selecting data, the mapper writes the country as the key, and the year and 
	 * percentage, in a string separated by double %. If there is no data then the 
	 * year is set to  -1 and the data 0.0. The assumption being that a lack of data 
	 * means that further research is required. 
	 *
	 * </p>
	 * 
	 * @param key		the LongWritable key assigned by the splitter
	 * @param text		the Text Value that contains one line of the csv
	 * @param context	the Context object
	 * 
	 * 
	 */
	
	
	@Override 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				
		/*
		 * 	Convert line (Text) to a String array where each "cell" is array cell.
		 * */
		String line = value.toString();
		String[] values = line.split("\",\"?", -1);
		
		
		
		String search = "SE.TER.CMPL.FE.ZS"; // Tertiary education, gross completion ratio, female
		String country;
		int lookBack = 5; // how far you want to look back 
		
		if (values[3].equals(search)) {
			values = Arrays.copyOf(values, 61);
			country = values[0].substring(1); //clean the name of the country
			int length = values.length;
			lab1: for (int i = 1; i <= lookBack; ++i) { // look back over the last 5 years
				String holder = values[length - i];
				if(holder != null && !holder.isEmpty()) {  // if data
					// list is (year, datum)
					int year = 2017 - i; // 2016+1
					String data = "" + year + "%%" + holder;
					Text out = new Text();
					out.set(data);
					context.write(new Text(country), out);  //broadcast the dataS
					break lab1;
				} else if (i == lookBack) { // if no data 
					String data = "-1;0.0";
					Text out = new Text();
					out.set(data);
					context.write(new Text(country), out); // broadcast that there was no data
				}	
			}	
		}
	}
}
