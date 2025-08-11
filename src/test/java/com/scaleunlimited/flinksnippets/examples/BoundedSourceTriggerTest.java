package com.scaleunlimited.flinksnippets.examples;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class BoundedSourceTriggerTest {

    @Test
    public void testBoundedSourceTriggering() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        
        final String inputFile = "src/test/resources/word-count-data.txt";
        DataStream<String> lines = env.readTextFile(inputFile, "UTF-8");
        
        lines
            .flatMap(new SplitLineFunction())
            .keyBy(t -> t.f0)
            .sum(1)
            .print();
        
        env.execute();
    }
    
    @SuppressWarnings("serial")
    private static class SplitLineFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : in.split(" ")) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }
}
