package com.scaleunlimited.flinksnippets;

import java.util.PriorityQueue;
import java.util.Random;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class MergeAndSortStreamsTest {

    @Test
    public void testMergeAndSort() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        DataStream<Event> streamA = env.addSource(new EventSource("A"))
                .assignTimestampsAndWatermarks(new EventTSWAssigner());
        DataStream<Event> streamB = env.addSource(new EventSource("B"))
                .assignTimestampsAndWatermarks(new EventTSWAssigner());
        
        streamA.union(streamB)
        .keyBy(r -> r.getKey())
        .process(new SortByTimestampFunction())
        .print();
        
        env.execute();
    }
    
    private static class Event implements Comparable<Event> {
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

        public String getKey() {
            return "1";
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
    private static class EventTSWAssigner extends AscendingTimestampExtractor<Event> {

        @Override
        public long extractAscendingTimestamp(Event element) {
            return element.getTimestamp();
        }
    }
    
    @SuppressWarnings("serial")
    private static class SortByTimestampFunction extends KeyedProcessFunction<String, Event, Event> {
        private ValueState<PriorityQueue<Event>> queueState = null;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<PriorityQueue<Event>> descriptor = new ValueStateDescriptor<>(
                    // state name
                    "sorted-events",
                    // type information of state
                    TypeInformation.of(new TypeHint<PriorityQueue<Event>>() {
                    }));
            queueState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Event event, Context context, Collector<Event> out) throws Exception {
            TimerService timerService = context.timerService();

            long currentWatermark = timerService.currentWatermark();
            System.out.format("processElement called with watermark %d\n", currentWatermark);
            if (context.timestamp() > currentWatermark) {
                PriorityQueue<Event> queue = queueState.value();
                if (queue == null) {
                    queue = new PriorityQueue<>(10);
                }
                
                queue.add(event);
                queueState.update(queue);
                timerService.registerEventTimeTimer(event.getTimestamp());
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Event> out) throws Exception {
            PriorityQueue<Event> queue = queueState.value();
            long watermark = context.timerService().currentWatermark();
            System.out.format("onTimer called  with watermark %d\n", watermark);
            Event head = queue.peek();
            while (head != null && head.getTimestamp() <= watermark) {
                out.collect(head);
                queue.remove(head);
                head = queue.peek();
            }
        }
    }
    
    @SuppressWarnings("serial")
    private static class EventSource extends RichParallelSourceFunction<Event> {

        private String _prefix;
        
        private transient Random _rand;
        private transient boolean _running;
        private transient int _numEvents;
        
        public EventSource(String prefix) {
            _prefix = prefix;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            _rand = new Random(_prefix.hashCode() + getRuntimeContext().getIndexOfThisSubtask());
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
                
                context.collect(new Event(_prefix, timestamp));
                _numEvents++;
                
                // Generate a timestamp every 5...15 ms, average is 10.
                timestamp += (5 + _rand.nextInt(10));
            }
        }
        
    }
}
