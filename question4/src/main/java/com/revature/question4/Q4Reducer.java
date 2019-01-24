package com.revature.question4;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

public class Q4Reducer extends Reducer<Text, Text, Text, Text>{
	/**
	 * Question 1 Reducer
	 * <p>
	 * 	The Question 1 Reducer receives the key, value pair and then splits the value pair
	 * in to year and data. Then sorts the values by year takes the first and last data pair
	 * and finds the percent deference. Then key value pair of country and the precent is written
	 * to the context. 
	 * </p>
	 * 
	 * 
	 */
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException{
		//log.traceEntry();
		List<YearData> dataList = new ArrayList<YearData>();
		for (Text val : values) {
			String[] strArr = val.toString().split("%%");
			Integer year = Integer.parseInt(strArr[0]);
			Double percent = Double.parseDouble(strArr[1]);
			YearData yd = new YearData(year,percent);
			dataList.add(yd);
		}
		
		dataList.sort(Comparator.comparingInt(YearData::getYear));
		
		
		int dlLength = dataList.size();
		double oldStore = dataList.get(0).getData();
		double newStore = dataList.get(dlLength - 1).getData();
		
		double answer = ((newStore - oldStore) / oldStore)*100;
		DecimalFormat myFormatter = new DecimalFormat("#0.000#");
		String output = myFormatter.format(answer);
		
		int oldYear = dataList.get(0).getYear();
		int newYear = dataList.get(dlLength-1).getYear();
		
		String finalOut = "%%" + oldYear + "%%" + newYear + "%%" + output ; 
		context.write(key, new Text(finalOut));
	}
}
