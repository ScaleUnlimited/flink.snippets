package com.scaleunlimited.flinksnippets;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class UnkeyedStateTest {

    @Test
    public void testUnkeyedMapState() throws Exception {
        LocalStreamEnvironment env = new LocalStreamEnvironment();
        env.setParallelism(2);

        env.fromElements(
                "192.168.1.0",
                "192.168.1.0",
                "192.168.1.1",
                "192.168.1.2",
                "192.168.1.3",
                "192.168.1.4",
                "192.168.1.5")

            .process(new MyProcessFunctionWithMapState())
            .print();
            
        try {
            env.execute();
            fail("Should have failed");
        } catch (Exception e) {
            // Expected
        }
    }

    @SuppressWarnings("serial")
    private static class MyProcessFunctionWithMapState extends ProcessFunction<String, Tuple2<String, Integer>> {

        private transient MapState<String, Integer> ipToCount;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            ipToCount = getRuntimeContext().getMapState(new MapStateDescriptor<>("ip-to-count", String.class, Integer.class));
        }
        
        @Override
        public void processElement(String in, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer curCount = ipToCount.get(in);
            if (curCount == null) {
                curCount = 0;
            }
            
            ipToCount.put(in, curCount + 1);
        }

    }


    @Test
    public void testUnkeyedManagedOperatorState() throws Exception {
        LocalStreamEnvironment env = new LocalStreamEnvironment();
        env.setParallelism(2);

        env.fromElements(
                "192.168.1.0",
                "192.168.1.0",
                "192.168.1.1",
                "192.168.1.2",
                "192.168.1.3",
                "192.168.1.4",
                "192.168.1.5")

            .process(new MyProcessFunction())
            .print();

        env.execute();
    }

    @SuppressWarnings("serial")
    private static class MyProcessFunction extends ProcessFunction<String, Tuple2<String, Integer>> implements CheckpointedFunction {

        private transient ListState<Tuple2<String, Integer>> ipToCountState;

        private transient Map<String, Integer> ipToCount;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            ipToCount = new HashMap<>();
        }
        
        @Override
        public void processElement(String in, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer curCount = ipToCount.get(in);
            if (curCount == null) {
                curCount = 0;
            }
            
            ipToCount.put(in, curCount + 1);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                    new ListStateDescriptor<>(
                            "ip-to-count",
                            TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

            ipToCountState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                for (Tuple2<String, Integer> element : ipToCountState.get()) {
                    ipToCount.put(element.f0, element.f1);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            ipToCountState.clear();
            
            for (Entry<String, Integer> element : ipToCount.entrySet()) {
                ipToCountState.add(new Tuple2<>(element.getKey(), element.getValue()));
            }
        }
        
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                Collector<Tuple2<String, Integer>> out) throws Exception {
            // TODO Auto-generated method stub
            super.onTimer(timestamp, ctx, out);
        }
    }

}
