package com.scaleunlimited.flinksnippets.tools;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class BroadcastStreamTest {

	@Test
	public void testUnionOfBroadcastAndKeyedStream() throws Exception {
		LocalStreamEnvironment env = new LocalStreamEnvironment();
		env.setParallelism(2);
		
		DataStreamSource<Tuple2<String, Float>> pages = env.fromElements(Tuple2.of("page0", 0.0f), Tuple2.of("page0", 1.0f), Tuple2.of("page1", 10.0f), Tuple2.of("page666", 6660.0f));
		DataStreamSource<Float> epsilon = env.fromElements(5.0f, 10.0f);
		
		DataStream<Tuple2<String, Float>> epsilonAsPages = epsilon.flatMap(new CreateBroadcastStream());
		DataStream<Tuple2<String, Float>> unioned = pages.union(epsilonAsPages).partitionCustom(new MyPartitioner(), 0).process(new MyProcessFunction());
		unioned.print();
		
		env.execute();
	}
	
	@SuppressWarnings("serial")
	private static class CreateBroadcastStream extends RichFlatMapFunction<Float, Tuple2<String, Float>> {

		private transient int _parallelism;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			
			RuntimeContext context = getRuntimeContext();
			_parallelism = context.getNumberOfParallelSubtasks();
		}
		
		@Override
		public void flatMap(Float mass, Collector<Tuple2<String, Float>> collector) throws Exception {
			for (int i = 0; i < _parallelism; i++) {
				collector.collect(new Tuple2<String, Float>("epsilon" + i, mass / _parallelism));
			}
			
		}

		
	}
	
	@SuppressWarnings("serial")
	private static class MyPartitioner implements Partitioner<String> {

		@Override
		public int partition(String key, int numPartitions) {
			if (key.startsWith("epsilon")) {
				return Integer.parseInt(key.substring("epsilon".length()));
			} else {
				return Math.abs(key.hashCode()) % numPartitions;
			}
		}
	}
	
	@SuppressWarnings("serial")
	private static class MyProcessFunction extends ProcessFunction<Tuple2<String, Float>, Tuple2<String, Float>> {

		@Override
		public void processElement(Tuple2<String, Float> in, Context context, Collector<Tuple2<String, Float>> collector) throws Exception {
			collector.collect(in);
		}
	}

}
