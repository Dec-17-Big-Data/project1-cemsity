package com.revature.project1.FemGrad;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class FemGradMapper extends Mapper <LongWritable,Text,Text,List<Writable>>{ //
	
	@Override 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				
		/* 
		 * 	Convert line (Text) to a String array where each "cell" is array cell.
		 * */
		String line = value.toString();
		String[] values = line.split("\",\"?");
		
		String search = "SE.TER.CMPL.FE.ZS"; // Tertiary education, gross completion ratio, female
		String country;
		int lookBack = 5; // how far you want to look back 
		
		if (values[3].equals(search)) {
			country = values[0].substring(1); //clean the name of the country
			int index = values.length;
			lab1: for (int i = 1; i <= lookBack; ++i) { // look back over the last 5 yearss
				String holder = values[index - i];
				if(holder != null) {  // if data
					List<Writable> out = new ArrayList<Writable>();
					// list is (year, datum)
					int year = 2017 - i;
					out.add(new IntWritable(year));
					out.add(new DoubleWritable(Double.parseDouble(holder)));
					
					context.write(new Text(country), out);  //broadcast the data
					break lab1;
				} else if (i == lookBack) { // if no data 
					List<Writable> out = new ArrayList<Writable>();
					// -1 tells reducer that data was not found
					out.add(new IntWritable(-1));
					out.add(new DoubleWritable(0.0));
						
					context.write(new Text(country), out); // broadcast no data
				}	
			}	
		}
	}
}
