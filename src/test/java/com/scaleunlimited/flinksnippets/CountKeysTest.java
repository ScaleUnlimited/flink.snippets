package com.scaleunlimited.flinksnippets;

import java.util.Random;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.junit.Test;

public class CountKeysTest {
    
    @Test
    public void testQueryableKeyCount() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        
        env.addSource(new EventSource())
               .keyBy(r -> r.getLabel())
               .filter(new MyCountFunction())
               .print();
               
        env.execute();
    }
    
    @SuppressWarnings("serial")
    private static class MyCountFunction extends RichFilterFunction<Event> {
        
        private ValueState<Long> counter = null;

        @Override
        public void open(Configuration config) throws Exception {
            super.open(config);
            
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                    // state name
                    "key-counts",
                    // type information of state
                    TypeInformation.of(new TypeHint<Long>() {
                    }));
            descriptor.setQueryable("key-counts");

            counter = getRuntimeContext().getState(descriptor);
        }

        @Override
        public boolean filter(Event in) throws Exception {
            Long count = counter.value();
            if (count == null) {
                count = new Long(0);
            }
            
            counter.update(count + 1);

            return true;
        }
    }
    
    public static class Event implements Comparable<Event> {
        private String _label;
        private long _timestamp;
        
        public Event(String label, long timestamp) {
            _label = label;
            _timestamp = timestamp;
        }

        public String getLabel() {
            return _label;
        }

        public void setLabel(String label) {
            _label = label;
        }

        public long getTimestamp() {
            return _timestamp;
        }

        public void setTimestamp(long timestamp) {
            _timestamp = timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("%s @ %d", _label, _timestamp);
        }

        @Override
        public int compareTo(Event o) {
            return Long.compare(_timestamp, o._timestamp);
        }
    }
    
    @SuppressWarnings("serial")
    private static class EventSource extends RichParallelSourceFunction<Event> {

        private transient Random _rand;
        private transient boolean _running;
        private transient int _numEvents;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            _rand = new Random(getRuntimeContext().getIndexOfThisSubtask());
        }
        
        @Override
        public void cancel() {
            _running = false;
        }

        @Override
        public void run(SourceContext<Event> context) throws Exception {
            _running = true;
            _numEvents = 0;
            long timestamp = System.currentTimeMillis() + _rand.nextInt(10);
            
            while (_running && (_numEvents < 100)) {
                long deltaTime = timestamp - System.currentTimeMillis();
                if (deltaTime > 0) {
                    Thread.sleep(deltaTime);
                }
                
                context.collect(new Event("key-" + _rand.nextInt(10), timestamp));
                _numEvents++;
                
                // Generate a timestamp every 5...15 ms, average is 10.
                timestamp += (5 + _rand.nextInt(10));
            }
        }
        
    }

}
