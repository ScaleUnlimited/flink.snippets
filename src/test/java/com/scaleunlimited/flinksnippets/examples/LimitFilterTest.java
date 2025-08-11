package com.scaleunlimited.flinksnippets.examples;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;
import org.junit.Test;

public class LimitFilterTest {

    @Test
    public void test() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        
        LongValueCollector.clearQueue();
        
        final long startValue = 0;
        final long endValue = 100;
        final int limit = 9;
        env.fromParallelCollection(new LongValueSequenceIterator(startValue, endValue), LongValue.class)
        .rebalance()
        .filter(new LimitFilter<LongValue>(limit))
        .addSink(new LongValueCollector());
        
        env.execute();
        
        Set<LongValue> results = new HashSet<>();
        for (LongValue v : LongValueCollector.getQueue()) {
            assertTrue(results.add(v));
        }
        
        assertEquals(limit, results.size());
    }

    @SuppressWarnings("serial")
    private static class LongValueCollector extends RichSinkFunction<LongValue> {
        
        private static final Queue<LongValue> QUEUE = new ConcurrentLinkedQueue<>();
        
        protected static void clearQueue() {
            QUEUE.clear();
        }
        
        protected static Queue<LongValue> getQueue() {
            return QUEUE;
        }
        
        @Override
        public void invoke(LongValue value, Context context) throws Exception {
            QUEUE.add(value);
        }
    }
}
