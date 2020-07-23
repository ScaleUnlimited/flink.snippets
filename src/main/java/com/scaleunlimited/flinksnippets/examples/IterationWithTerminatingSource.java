package com.scaleunlimited.flinksnippets.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class IterationWithTerminatingSource {
    
    @SuppressWarnings("serial")
    public static void build(StreamExecutionEnvironment env, SourceFunction<Integer> source) {
        IterativeStream<Integer> iter = env.addSource(source).iterate(1000L);
        
        DataStream<Integer> updated = iter.flatMap(new FlatMapFunction<Integer, Integer>() {

            private boolean _keepGoing = true;
            
            @Override
            public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                if (value == -1) {
                    _keepGoing = false;
                }
                
                if (_keepGoing) {
                    Thread.sleep(100);
                    out.collect(value + 1);
                }
            }
        });
        
        iter.closeWith(updated)
            .print();
    }
}
