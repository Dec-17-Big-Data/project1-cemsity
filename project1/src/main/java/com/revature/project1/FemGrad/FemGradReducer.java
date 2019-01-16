package com.revature.project1.FemGrad;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

public class FemGradReducer extends Reducer<Text, Iterable<Text>, Text, Text>{
	//private static Logger log = LogManager.getLogger(FemGradReducer.class);
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException{
		//log.traceEntry();
		for (Text val : values) {
			Text test = new Text("Hello World!");
			context.write(test, test);
		}
	}
}

