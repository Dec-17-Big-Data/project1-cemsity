package com.revature.question4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4Driver {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		if (args.length != 2) {
			System.out.printf(
					"Usage: Female Employment <input dir> <output dir>\n");
			System.exit(-1);
		}
		Job job = new Job();
		
		job.setJarByClass(Q4Driver.class);
		job.setJobName("Female Employment");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
	    job.setMapperClass(Q4Mapper.class);
		job.setReducerClass(Q4Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
