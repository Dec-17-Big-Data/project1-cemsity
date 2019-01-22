package com.revature.question2;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Q2Reducer extends Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException{
		List<YearData> ydList = new ArrayList<YearData>();
		
		for(Text value : values) {
			ydList.add(new YearData(value.toString()));
		}
		
		ydList.sort(Comparator.comparingInt(YearData::getYear));
		
		YearData ydLast = null;
		for (YearData yd : ydList ) {
			if (ydLast == null) {
				ydLast = yd;
				break;
			}
			Double lastData = ydLast.getData();
			Double thisData = yd.getData();
			double answer = ((thisData - lastData) / lastData) * 100;
			DecimalFormat myFormatter = new DecimalFormat("#0.000#");
			String output = myFormatter.format(answer);
			Text outValue = new Text(output);
			
			Integer lastYear = ydLast.getYear();
			Integer thisYear = yd.getYear();
			
			Text outKey = new Text("" + lastYear + "-" + thisYear);
			
			context.write(outKey, outValue);
		}
		
	}
}