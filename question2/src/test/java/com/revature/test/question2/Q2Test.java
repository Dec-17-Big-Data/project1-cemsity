package com.revature.test.question2;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.types.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.question2.Q2Mapper;
import com.revature.question2.Q2Reducer;

public class Q2Test {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text ,Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrDriver;
	
	
	@Before
	public void setUp() {
		//Mapper
		Q2Mapper mapper = new Q2Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		//Reducer
		
		Q2Reducer reducer = new Q2Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		//MapReduce
		mrDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mrDriver.setMapper(mapper);
		mrDriver.setReducer(reducer);
	}
	@Test
	public void testMapper() throws Exception{
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\",\"37.43131\",\"38.22037\",\"39.18913\",\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\",\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\",\"\",\"\",\"\",\"\","));
	
		Text outKey = new Text("USA");
		Text outVal = new Text("2012%%47.68032");
		Pair<Text, Text> out = new Pair<Text,Text>(outKey, outVal);
		 
		final List<Pair<Text,Text>> result = mapDriver.run();
		
		assertThat(result, notNullValue());
		assertThat(result, hasItem(out));
		assertThat(result.size(), equalTo(12));
			
	}
	@Test
	public void testReducer() throws Exception{
		List<Text> value1 = new ArrayList<Text>();
		value1.add(new Text("2011%%46.37914"));
		value1.add(new Text("2012%%47.68032"));
		
		reduceDriver.withInput(new Text("USA"), value1);
		
		reduceDriver.withOutput(new Text("2011-2012"), new Text("2.8055"));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws Exception{
		mrDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\",\"37.43131\",\"38.22037\",\"39.18913\",\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\",\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\",\"\",\"\",\"\",\"\","));
		
		Text outKey = new Text("2000-2001");
		Text outVal = new Text("-1.0534");
		Pair<Text, Text> out = new Pair<Text,Text>(outKey, outVal);
		
		final List<Pair<Text,Text>> result = mrDriver.run();
		
		assertThat(result, notNullValue());
		assertThat(result, hasItem(out));
		assertThat(result.size(), equalTo(11));
		
	}
}
