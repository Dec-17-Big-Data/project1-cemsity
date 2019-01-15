package com.revature.project1.FemGrad;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class FemGradReducer extends Reducer<Text, List<Writable>, Text, Text>{
	
	public void reduce(Text key, List<Writable> values, Context context ) throws IOException, InterruptedException{
		
		DoubleWritable datumDW = (DoubleWritable) values.get(1);
		if (datumDW.get() < 30.0) {
			IntWritable yearIW = (IntWritable) values.get(0);
			String out = "(" + yearIW.toString() + "," + datumDW.toString() + ")";
			Text value = new Text(out);
			context.write(key, value);
		}	
	}
}
