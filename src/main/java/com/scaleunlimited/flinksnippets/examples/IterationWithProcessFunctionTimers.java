package com.scaleunlimited.flinksnippets.examples;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinksnippets.examples.IterationWithProcessFunctionTimersTest.TimedTerminator;

public class IterationWithProcessFunctionTimers {
    static final Logger LOGGER = LoggerFactory.getLogger(IterationWithProcessFunctionTimers.class);

    public static void build(StreamExecutionEnvironment env, SourceFunction<String> source, TimedTerminator terminator) {
        IterativeStream<String> iter = env.addSource(source).iterate(1000L);
        
        @SuppressWarnings("serial")
        DataStream<String> updated = iter.keyBy(new KeySelector<String, String>() {

            @Override
            public String getKey(String value) throws Exception {
                return value.substring(0, value.indexOf(':'));
            }
        }).process(new MyKeyedProcessFunction(terminator));
        
        iter.closeWith(updated)
            .print();
    }
    
    @SuppressWarnings("serial")
    private static class MyKeyedProcessFunction extends KeyedProcessFunction<String, String, String> implements CheckpointedFunction {

        private TimedTerminator _terminator;
        
        private transient int _numResults;
        private transient ListState<String> _entries;
        private transient ListState<Boolean> _emitted;
        private transient ValueState<Integer> _index;
        private transient ValueState<Integer> _size;
        
        public MyKeyedProcessFunction(TimedTerminator terminator) {
            _terminator = terminator;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            _numResults = 0;
            
            _terminator.open();
        }
        
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            if (_numResults > 200) {
                LOGGER.error("Pretending to fail, so throw an exception while processing {}", value);
                throw new RuntimeException("Example of a failure triggering a job restart");
            }
            
            // See if we have this element already. If not, create a timer.
            long processingTime = ctx.timerService().currentProcessingTime();
            if (_size.value() == null) {
                _entries.add(value);
                _emitted.add(false);
                _index.update(0);
                _size.update(1);
                
                // And we want to create a timer, so we have one per domain
                ctx.timerService().registerProcessingTimeTimer(processingTime + 50);
            } else {
                // We should only get called with new entries, due to the tracking of
                // state that onTimer() does, so we can just blindly add it.
                _entries.add(value);
                _emitted.add(false);
                _size.update(_size.value() + 1);;
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            
            if (!_terminator.isStopTime()) {
                ctx.timerService().registerProcessingTimeTimer(timestamp + 50);
                
                // Increment our index
                int curIndex = _index.value();
                
                curIndex += 1;
                if (curIndex >= _size.value()) {
                    curIndex = 0;
                }
                
                // Update for next time we're called.
                _index.update(curIndex);
                
                // Now find the <curIndex>th value.
                Iterator<String> entryIter = _entries.get().iterator();
                for (int i = 0; i < curIndex; i++) {
                    entryIter.next();
                }
                
                List<Boolean> flags = new ArrayList<>();
                for (Boolean emitted : _emitted.get()) {
                    flags.add(emitted);
                }
                
                if (!flags.get(curIndex)) {
                    String entry = entryIter.next();
                    _numResults += 2;
                    out.collect(entry + "page0/");
                    out.collect(entry + "page1/");
                    
                    flags.set(curIndex, true);
                    _emitted.update(flags);
                }
            }
        }
        
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            ListStateDescriptor<String> entriesDescriptor = new ListStateDescriptor<>(
                    "entries", String.class);
            _entries = getRuntimeContext().getListState(entriesDescriptor);
            
            ListStateDescriptor<Boolean > emittedDescriptor = new ListStateDescriptor<>(
                    "emitted", Boolean.class);
            _emitted = getRuntimeContext().getListState(emittedDescriptor);
            
            ValueStateDescriptor<Integer> indexDescriptor = new ValueStateDescriptor<>(
                    "index", Integer.class);
            _index = getRuntimeContext().getState(indexDescriptor);
            
            ValueStateDescriptor<Integer> sizeDescriptor = new ValueStateDescriptor<>(
                    "size", Integer.class);
            _size = getRuntimeContext().getState(sizeDescriptor);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            // Nothing special we need to do here, since we have managed keyed state that gets
            // handled automatically.
        }
        
    }

    /**
     * Version of MyKeyedProcessFunction that's needed if running with Flink 1.4.x, since KeyedProcessFunction
     * was only introduced in 1.5
     *
     */
    @SuppressWarnings("serial")
    private static class MyProcessFunction extends ProcessFunction<String, String> implements CheckpointedFunction {

        private TimedTerminator _terminator;
        
        private transient int _numResults;
        private transient boolean _failed;
        private transient int _taskId;
        
        private transient ListState<String> _entries;
        private transient ListState<Boolean> _emitted;
        private transient ValueState<Integer> _index;
        private transient ValueState<Integer> _size;
        
        public MyProcessFunction(TimedTerminator terminator) {
            _terminator = terminator;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            _numResults = 0;
            _failed = false;
            _taskId = getRuntimeContext().getIndexOfThisSubtask();
            
            _terminator.open();
        }
        
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            if ((_numResults > 800) && !_failed) {
                LOGGER.error("Oops, we have a failure for task {}", _taskId);
                _failed = true;
                throw new IllegalStateException("Causing failure to trigger restart");
            }
            
            // See if we have this element already. If not, create a timer.
            long processingTime = ctx.timerService().currentProcessingTime();
            if (_size.value() == null) {
                _entries.add(value);
                _emitted.add(false);
                _index.update(0);
                _size.update(1);
                
                ctx.timerService().registerProcessingTimeTimer(processingTime + 50);
            } else {
                // see if we already have the element. If not, we need to add it.
                Iterable<String> iter = _entries.get();
                for (String entry : iter) {
                    if (entry.equals(value)) {
                        return;
                    }
                }
                
                _entries.add(value);
                _emitted.add(false);
                _size.update(_size.value() + 1);;

            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            
            if (!_terminator.isStopTime()) {
                ctx.timerService().registerProcessingTimeTimer(timestamp + 50);
                
                // Increment our index
                int curIndex = _index.value();
                
                curIndex += 1;
                if (curIndex >= _size.value()) {
                    curIndex = 0;
                }
                
                _index.update(curIndex);
                
                // Now find the <curIndex>th value.
                Iterator<String> entryIter = _entries.get().iterator();
                Iterator<Boolean> emittedIter = _emitted.get().iterator();
                while (curIndex > 0) {
                    entryIter.next();
                    emittedIter.next();
                    curIndex--;
                }
                
                if (!emittedIter.next()) {
                    String entry = entryIter.next();
                    _numResults += 2;
                    out.collect(entry + "page0/");
                    out.collect(entry + "page1/");
                }
            }
        }
        
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            ListStateDescriptor<String> entriesDescriptor = new ListStateDescriptor<>(
                    "entries", String.class);
            _entries = getRuntimeContext().getListState(entriesDescriptor);
            
            ListStateDescriptor<Boolean > emittedDescriptor = new ListStateDescriptor<>(
                    "emitted", Boolean.class);
            _emitted = getRuntimeContext().getListState(emittedDescriptor);
            
            ValueStateDescriptor<Integer> indexDescriptor = new ValueStateDescriptor<>(
                    "index", Integer.class);
            _index = getRuntimeContext().getState(indexDescriptor);
            
            ValueStateDescriptor<Integer> sizeDescriptor = new ValueStateDescriptor<>(
                    "size", Integer.class);
            _size = getRuntimeContext().getState(sizeDescriptor);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            // Nothing special we need to do here, since we have managed keyed state that gets
            // handled automatically.
        }
        
    }
}
