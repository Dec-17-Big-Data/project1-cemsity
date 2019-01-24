package com.revature.test.question4;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


import com.revature.question4.Q4Mapper;
import com.revature.question4.Q4Reducer;

public class Q4Test {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text ,Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrDriver;
	
	
	@Before
	public void setUp() {
		//Mapper
		Q4Mapper mapper = new Q4Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		//Reducer
		
		Q4Reducer reducer = new Q4Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		//MapReduce
		mrDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mrDriver.setMapper(mapper);
		mrDriver.setReducer(reducer);
	}
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("\"Czech Republic\",\"CZE\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"51.1030006408691\",\"50.2970008850098\",\"49.4959983825684\",\"49.8680000305176\",\"49.7999992370605\",\"49.3409996032715\",\"49.0929985046387\",\"48.0110015869141\",\"46.9469985961914\",\"46.3699989318848\",\"46.5660018920898\",\"46.8450012207031\",\"46.1479988098145\",\"45.8660011291504\",\"45.8320007324219\",\"46.1730003356934\",\"46.5530014038086\",\"46.6139984130859\",\"45.7109985351563\",\"45.1069984436035\",\"45.5550003051758\",\"45.976001739502\",\"46.7729988098145\",\"47.1969985961914\",\"47.9980010986328\",\"48.6580009460449\","));
	
		mapDriver.withOutput(new Text("Czech Republic"), new Text("2000%%46.3699989318848"));
		mapDriver.withOutput(new Text("Czech Republic"), new Text("2016%%48.6580009460449"));
		mapDriver.runTest(false);
	}
	@Test
	public void testReducer() {
		List<Text> value1 = new ArrayList<Text>();
		value1.add(new Text("2000%%46.3699989318848"));
		value1.add(new Text("2016%%48.6580009460449"));
		
		reduceDriver.withInput(new Text("Czech Republic"), value1);
		
		reduceDriver.withOutput(new Text("Czech Republic"), new Text("%%2000%%2016%%4.9342"));
		
		reduceDriver.runTest(false);
	}
	
	@Test
	public void testMapReduce() {
		mrDriver.withInput(new LongWritable(1), new Text("\"Czech Republic\",\"CZE\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"51.1030006408691\",\"50.2970008850098\",\"49.4959983825684\",\"49.8680000305176\",\"49.7999992370605\",\"49.3409996032715\",\"49.0929985046387\",\"48.0110015869141\",\"46.9469985961914\",\"46.3699989318848\",\"46.5660018920898\",\"46.8450012207031\",\"46.1479988098145\",\"45.8660011291504\",\"45.8320007324219\",\"46.1730003356934\",\"46.5530014038086\",\"46.6139984130859\",\"45.7109985351563\",\"45.1069984436035\",\"45.5550003051758\",\"45.976001739502\",\"46.7729988098145\",\"47.1969985961914\",\"47.9980010986328\",\"48.6580009460449\","));
		
		mrDriver.addOutput(new Text("Czech Republic"), new Text("%%2000%%2016%%4.9342"));
		
		mrDriver.runTest(false);
	}
}
