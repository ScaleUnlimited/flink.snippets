package com.scaleunlimited.flinksnippets;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class FilterWordsByCountTest {

    @Test
    public void test() throws Exception {
        LocalStreamEnvironment env = new LocalStreamEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9000)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public void flatMap(String text, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String word : text.split("[ ]+")) {
                            collector.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })
                
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                .filter(word -> word.f1 >= 2);
        
        dataStream.print();
        
        env.execute();
    }
}
