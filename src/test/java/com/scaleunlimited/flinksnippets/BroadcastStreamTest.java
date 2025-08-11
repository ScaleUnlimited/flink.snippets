package com.scaleunlimited.flinksnippets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class BroadcastStreamTest {

    private static MapStateDescriptor<String, String> REMAPPING_STATE = new MapStateDescriptor<>("remappings", String.class, String.class);

    
    @Test
    public void testUnkeyedStreamWithBroadcastStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        List<Tuple2<String, String>> attributeRemapping = new ArrayList<>();
        attributeRemapping.add(new Tuple2<>("one", "1"));
        attributeRemapping.add(new Tuple2<>("two", "2"));
        attributeRemapping.add(new Tuple2<>("three", "3"));
        attributeRemapping.add(new Tuple2<>("four", "4"));
        attributeRemapping.add(new Tuple2<>("five", "5"));
        attributeRemapping.add(new Tuple2<>("six", "6"));
        
        BroadcastStream<Tuple2<String, String>> attributes = env.fromCollection(attributeRemapping)
                .broadcast(REMAPPING_STATE);
        
        List<Map<String, Integer>> xmlData = new ArrayList<>();
        xmlData.add(makePOJO("one", 10));
        xmlData.add(makePOJO("two", 20));
        xmlData.add(makePOJO("three", 30));
        xmlData.add(makePOJO("four", 40));
        xmlData.add(makePOJO("five", 50));

        DataStream<Map<String, Integer>> records = env.fromCollection(xmlData);
        
        records.connect(attributes)
            .process(new MyRemappingFunction())
            .print();
        
        env.execute();
    }

    private Map<String, Integer> makePOJO(String key, int value) {
        Map<String, Integer> result = new HashMap<>();
        result.put(key, value);
        return result;
    }
    
    @SuppressWarnings("serial")
    private static class MyRemappingFunction extends BroadcastProcessFunction<Map<String, Integer>, Tuple2<String, String>, Map<String, Integer>> {

        @Override
        public void processBroadcastElement(Tuple2<String, String> in, Context ctx, Collector<Map<String, Integer>> out) throws Exception {
            ctx.getBroadcastState(REMAPPING_STATE).put(in.f0, in.f1);
        }

        @Override
        public void processElement(Map<String, Integer> in, ReadOnlyContext ctx, Collector<Map<String, Integer>> out) throws Exception {
            final ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(REMAPPING_STATE);

            Map<String, Integer> result = new HashMap<>();
            
            for (String key : in.keySet()) {
                if (state.contains(key)) {
                    result.put(state.get(key), in.get(key));
                } else {
                    result.put(key, in.get(key));
                }
            }
            
            out.collect(result);
        }
        
    }
}
