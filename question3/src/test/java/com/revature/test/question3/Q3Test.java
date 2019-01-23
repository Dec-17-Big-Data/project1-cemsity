package com.revature.test.question3;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


import com.revature.question3.Q3Mapper;
import com.revature.question3.Q3Reducer;

public class Q3Test {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text ,Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrDriver;
	
	
	@Before
	public void setUp() {
		//Mapper
		Q3Mapper mapper = new Q3Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		//Reducer
		
		Q3Reducer reducer = new Q3Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		//MapReduce
		mrDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mrDriver.setMapper(mapper);
		mrDriver.setReducer(reducer);
	}
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("\"Czech Republic\",\"CZE\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"69.4649963378906\",\"69.0940017700195\",\"68.7779998779297\",\"68.7750015258789\",\"68.9150009155273\",\"68.8669967651367\",\"68.3479995727539\",\"67.4729995727539\",\"65.4680023193359\",\"64.7490005493164\",\"64.8550033569336\",\"65.4250030517578\",\"64.8450012207031\",\"63.7620010375977\",\"64.4509963989258\",\"64.6669998168945\",\"65.5599975585938\",\"65.8570022583008\",\"64.2639999389648\",\"63.6609992980957\",\"63.5849990844727\",\"63.6339988708496\",\"64.1269989013672\",\"64.8470001220703\",\"65.3190002441406\",\"65.7740020751953\","));
	
		mapDriver.withOutput(new Text("Czech Republic"), new Text("2000%%64.7490005493164"));
		mapDriver.withOutput(new Text("Czech Republic"), new Text("2016%%65.7740020751953"));
		
		mapDriver.runTest();
	}
	@Test
	public void testReducer() {
		List<Text> value1 = new ArrayList<Text>();
		value1.add(new Text("2000%%64.7490005493164"));
		value1.add(new Text("2016%%65.7740020751953"));
		
		reduceDriver.withInput(new Text("Czech Republic"), value1);
		
		reduceDriver.withOutput(new Text("Czech Republic"), new Text("%%2000%%2016%%1.583"));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mrDriver.withInput(new LongWritable(1), new Text("\"Czech Republic\",\"CZE\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"69.4649963378906\",\"69.0940017700195\",\"68.7779998779297\",\"68.7750015258789\",\"68.9150009155273\",\"68.8669967651367\",\"68.3479995727539\",\"67.4729995727539\",\"65.4680023193359\",\"64.7490005493164\",\"64.8550033569336\",\"65.4250030517578\",\"64.8450012207031\",\"63.7620010375977\",\"64.4509963989258\",\"64.6669998168945\",\"65.5599975585938\",\"65.8570022583008\",\"64.2639999389648\",\"63.6609992980957\",\"63.5849990844727\",\"63.6339988708496\",\"64.1269989013672\",\"64.8470001220703\",\"65.3190002441406\",\"65.7740020751953\","));
			
		mrDriver.addOutput(new Text("Czech Republic"), new Text("%%2000%%2016%%1.583"));
		
		mrDriver.runTest();
	}
}
