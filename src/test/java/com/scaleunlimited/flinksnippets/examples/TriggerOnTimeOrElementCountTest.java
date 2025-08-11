package com.scaleunlimited.flinksnippets.examples;

import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class TriggerOnTimeOrElementCountTest {

    @Test
    public void test() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        
        @SuppressWarnings("serial")
        RichParallelSourceFunction<Integer> source = new RichParallelSourceFunction<Integer>() {

            private boolean _keepRunning = true;
            private int _taskId;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                
                _taskId = ((StreamingRuntimeContext)getRuntimeContext()).getIndexOfThisSubtask() * 10_000;
            }
            
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                Random rand = new Random(_taskId);
                
                final int totalElements = 100;
                int numElements = 0;
                long startTime = System.currentTimeMillis() - (totalElements * 1000L);
                while (_keepRunning) {
                    if (numElements < 100) {
                        ctx.collectWithTimestamp(1, startTime + (numElements * 1000L));
                        numElements++;
                    } else {
                        Thread.sleep(1000L);
                        break;
                    }
                }
            }

            @Override
            public void cancel() {
                _keepRunning = false;
            }
        };
        
        env.addSource(source)
            .keyBy(i -> i)
            .timeWindow(Time.seconds(10))
            .trigger(new MyTrigger())
            .process(new MyProcessFunction())
            .print();
        
        env.execute();
    }
    
    @SuppressWarnings("serial")
    private static class MyProcessFunction extends ProcessWindowFunction<Integer, Tuple2<Integer, Integer>, Integer, TimeWindow> {

        @Override
        public void process(Integer key, Context ctx,
                Iterable<Integer> iter, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            
            int sum = 0;
            for (Integer in : iter) {
                sum += in;
            }
            
            out.collect(new Tuple2<>(key, sum));
        }
        
    }
    
    @SuppressWarnings("serial")
    private static class MyTrigger extends Trigger<Integer, TimeWindow> {

        private int numElements = 0;
        
        @Override
        public TriggerResult onElement(Integer element, long timestamp, TimeWindow window, TriggerContext ctx)
                throws Exception {
            numElements++;
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
                throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
                throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx)
                throws Exception {
            numElements = 0;
            
            System.out.println("Clearing trigger");
        }
        
    }
}
