package com.revature.question5;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Q5Mapper extends Mapper <LongWritable, Text, Text, Text>{
	/**
	 * Question 5 Mapper
	 * <p>
	 * For Question 5, map iterates over the document looking for lines that 
	 * contains data, and a check if certain Series codes contain refrences to 
	 * year spicific data. I the year 2014 as it is the most recent year with
	 * a most amount of data. 
	 * 
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
		values = Arrays.copyOf(values, 61);
		
		String search = "NY.GDP.PCAP.CD";
		String country = values[0].substring(1); 
		String data = values[58];
		String dataCode = values[3];
		
		if (data != null && !data.isEmpty() && (!dataCode.contains("WP11") || !dataCode.contains("WP15"))) {
			context.write(new Text(country), new Text("1%%1"));
		}
				
		if (dataCode.equals(search)) {
			if (data != null && !data.isEmpty()) {
				context.write(new Text(country), new Text("2%%" + data));
			} else {
				context.write(new Text(country), new Text("2%%-1"));
			}
		}
	}		
}

