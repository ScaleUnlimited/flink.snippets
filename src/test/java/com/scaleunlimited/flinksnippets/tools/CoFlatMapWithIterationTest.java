package com.scaleunlimited.flinksnippets.tools;

import java.util.ArrayList;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

public class CoFlatMapWithIterationTest {

	@Test
	public void testTerminationWithIteration() throws Exception {
		final int parallelism = 1;
		LocalStreamEnvironment env = new LocalStreamEnvironment();
		env.setParallelism(parallelism);
		
		DataStreamSource<Tuple2<String, String>> domainLinksSource = env.fromElements(
				Tuple2.of("domain0", "domain1"), 
				Tuple2.of("domain0", "domain2"), 
				Tuple2.of("domain1", "domain0"), 
				Tuple2.of("domain2", "domain0"));
		
		TupleTypeInfo<Tuple2<String, Float>> domainMassTypeInfo = new TupleTypeInfo<Tuple2<String, Float>>(
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.FLOAT_TYPE_INFO);

		DataStreamSource<Tuple2<String, Float>> domainMassSource = env.fromCollection(new ArrayList<Tuple2<String, Float>>(), domainMassTypeInfo);
		
		IterativeStream<Tuple2<String, Float>> domainMassIter = domainMassSource.iterate();
		
		domainMassIter.closeWith(domainLinksSource.connect(domainMassIter).process(new MyCoProcess()));
		
		try {
			env.execute();
		} catch (JobExecutionException e) {
			Assert.fail(e.getCause().getMessage());
		}
	}
	
	@SuppressWarnings("serial")
	private static class MyCoProcess extends CoProcessFunction<Tuple2<String, String>, Tuple2<String, Float>, Tuple2<String, Float>> {

		@Override
		public void processElement1(Tuple2<String, String> domainLink,
				Context context,
				Collector<Tuple2<String, Float>> collector) throws Exception {
			collector.collect(new Tuple2<String, Float>(domainLink.f0, 0.5f));
		}

		@Override
		public void processElement2(Tuple2<String, Float> domainMass,
				Context context,
				Collector<Tuple2<String, Float>> collector) throws Exception {
			System.out.println("Got mass for " + domainMass.f0);
			collector.collect(domainMass);
		}
	}
	
}
