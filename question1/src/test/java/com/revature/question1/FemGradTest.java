package com.revature.question1;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.question1.FemGrad.FemGradMapper;
import com.revature.question1.FemGrad.FemGradReducer;

public class FemGradTest {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text ,Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrDriver;
	
	
	@Before
	public void setUp() {
		//Mapper
		FemGradMapper mapper = new FemGradMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		//Reducer
		
		FemGradReducer reducer = new FemGradReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		//MapReduce
		mrDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mrDriver.setMapper(mapper);
		mrDriver.setReducer(reducer);
	}
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("\"Egypt, Arab Rep.\",\"EGY\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"19.63836\",\"19.53745\",\"21.01584\",\"22.07007\",\"25.87731\",\"25.51352\",\"25.57537\",\"25.8574\",\"23.22869\",\"24.32711\",\"\",\"\",\"25.83895\",\"\",\"\",\"\","));
	
		mapDriver.withOutput(new Text("Egypt, Arab Rep."), new Text("2013;25.83895"));
		
		mapDriver.runTest();
	}
	@Test
	public void testReducer() {
		List<Text> value1 = new ArrayList<Text>();
		value1.add(new Text("2013;25.83895"));
		
		reduceDriver.withInput(new Text("Egypt, Arab Rep."), value1);
		
		reduceDriver.withOutput(new Text("Egypt, Arab Rep."), new Text("2013;25.83895"));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mrDriver.withInput(new LongWritable(1), new Text("\"Egypt, Arab Rep.\",\"EGY\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"19.63836\",\"19.53745\",\"21.01584\",\"22.07007\",\"25.87731\",\"25.51352\",\"25.57537\",\"25.8574\",\"23.22869\",\"24.32711\",\"\",\"\",\"25.83895\",\"\",\"\",\"\","));
		
		mrDriver.addOutput(new Text("Egypt, Arab Rep."), new Text("2013;25.83895"));
		
		mrDriver.runTest();
	}
}
